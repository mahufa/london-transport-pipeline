import io
from pathlib import Path

from pytest import mark

from include.datasets import DATASETS, RECORD_KEY_FIELDS
from include.helpers.json_validator import Rejected, classify_record, generate_clean_lines

FIXTURE_PATH = Path(__file__).parent / 'fixtures'


@mark.parametrize('source', DATASETS)
def test_key_fields_match_fixture(source):
    dlq = []

    with open(FIXTURE_PATH / source / 'sample.json', 'rb') as f:
        rows = list(generate_clean_lines(f, dlq, RECORD_KEY_FIELDS[source]))

    assert rows
    assert dlq == []


@mark.parametrize(
    'record, expected_error',
    [
        (1, 'Not a JSON object'),
        ({'name': 'x'}, 'Missing record key'),
        ({'id': 'a', 'v': '\x00'}, 'Contains null bytes'),
        ({'id': 'a', 'v': '\ud800'}, 'Serialization error'),
    ],
    ids=['not_object', 'missing_key', 'null_byte', 'lone_surrogate'],
)
def test_classify_record_rejects(record, expected_error):
    result = classify_record(record, ('id',))

    assert isinstance(result, Rejected)
    assert result.error.startswith(expected_error)


def test_record_key_falls_back_to_alternative_field():
    result = classify_record({'k': 'x', 'alt2': 'y'}, ('k', ('alt1', 'alt2')))

    assert result.key == 'x,y'


def test_rejects_do_not_advance_ordinal():
    dlq = []
    stream = io.BytesIO(b'[{"id":"a"},1,{"id":"b"}]')

    rows = list(generate_clean_lines(stream, dlq, ('id',)))

    assert [(o, k) for o, k, _ in rows] == [(0, 'a'), (1, 'b')]
    assert len(dlq) == 1

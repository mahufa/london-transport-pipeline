import io

from pytest import mark, raises

from include.helpers.postgres import _escape_for_copy, _prepare_dlq_stream, _to_copy_row, stream_to_pg_with_dlq


@mark.parametrize(
    'value, expected',
    [
        ('a\\b', 'a\\\\b'),
        ('a\tb', 'a\\tb'),
        ('a\nb', 'a\\nb'),
        ('a\rb', 'a\\rb'),
        ('a\\tb', 'a\\\\tb'),
    ],
    ids=['backslash', 'tab', 'newline', 'carriage_return', 'literal_backslash_t'],
)
def test_escape_for_copy(value, expected):
    assert _escape_for_copy(value) == expected


def test_to_copy_row():
    assert _to_copy_row(0, 'k', 'p\tq') == b'0\tk\tp\\tq\n'


def test_prepare_dlq_stream():
    dlq_buffer = [
        ('{"a":"x\ty"}', 'Missing record key'),
        ('1', 'Not a JSON object'),
    ]

    lines = _prepare_dlq_stream(
        dlq_buffer,
        'batch',
        'roads'
    ).read().split('\n')

    assert lines == [
        'roads\tbatch\tingest\t{"a":"x\\ty"}\tMissing record key',
        'roads\tbatch\tingest\t1\tNot a JSON object',
    ]


def test_prepare_dlq_stream_empty_buffer():
    assert _prepare_dlq_stream([], 'batch', 'roads').read() == ''


def test_unknown_source_raises():
    with raises(KeyError):
        stream_to_pg_with_dlq(io.BytesIO(b'[]'), 'batch', 'unknown')
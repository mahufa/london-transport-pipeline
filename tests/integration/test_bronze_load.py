import io
from pathlib import Path

from pytest import mark

from include.datasets import DATASETS
from include.helpers.postgres import stream_to_pg_with_dlq

FIXTURE_PATH = Path(__file__).parents[1] / 'fixtures'
BATCH_ID = '240101T000000'
SOURCE = 'bike_points'


def test_load_routes_invalid_records_to_dlq(pg_conn):
    _load(pg_conn, b'[{"id":"a"},{"id":"b"},1]')

    assert _keys(pg_conn) == ['a', 'b']
    assert _errors(pg_conn) == ['Not a JSON object']


def test_rerun_is_idempotent(pg_conn):
    for _ in range(2):
        _load(pg_conn, b'[{"id":"a"},{"id":"b"},1]')

    assert _keys(pg_conn) == ['a', 'b']
    assert _errors(pg_conn) == ['Not a JSON object']


def test_rerun_replaces_batch(pg_conn):
    _load(pg_conn, b'[{"id":"a"},{"id":"b"}]')
    _load(pg_conn, b'[{"id":"a"}]')

    assert _keys(pg_conn) == ['a']


def test_duplicate_key_keeps_first_record(pg_conn):
    _load(pg_conn, b'[{"id":"a","v":1},{"id":"a","v":2}]')

    assert _query(pg_conn, "SELECT payload->>'v' FROM bronze.raw_tfl") == ['1']
    assert _errors(pg_conn) == ['duplicate record key']


@mark.parametrize('source', DATASETS)
def test_fixture_loads(pg_conn, source):
    with open(FIXTURE_PATH / source / 'sample.json', 'rb') as f:
        stream_to_pg_with_dlq(f, BATCH_ID, source, pg_conn)

    assert _keys(pg_conn)
    assert _errors(pg_conn) == []


def _load(pg_conn, data: bytes):
    stream_to_pg_with_dlq(io.BytesIO(data), BATCH_ID, SOURCE, pg_conn)


def _keys(pg_conn) -> list:
    return _query(pg_conn, 'SELECT record_key FROM bronze.raw_tfl ORDER BY record_key')


def _errors(pg_conn) -> list:
    return _query(pg_conn, 'SELECT error FROM bronze.rejected_records ORDER BY id')


def _query(pg_conn, sql: str) -> list:
    with pg_conn, pg_conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]

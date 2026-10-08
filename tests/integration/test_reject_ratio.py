import io
import json
from pathlib import Path

from include.helpers.postgres import stream_to_pg_with_dlq
from include.paths import SQL_DIR

FIXTURE = Path(__file__).parents[1] / 'fixtures' / 'bike_points' / 'sample.json'
BATCH_ID = '240101T000000'
SOURCE = 'bike_points'
MAX_REJECT_RATIO = 0.1


def test_clean_batch_passes(pg_conn):
    _load(pg_conn, _fixture_records())
    _reject_silver(pg_conn)

    assert _check(pg_conn) is True


def test_empty_batch_passes(pg_conn):
    _load(pg_conn, [])

    assert _check(pg_conn) is True


def test_all_rejected_at_ingest_fails(pg_conn):
    _load(pg_conn, [1, 2])

    assert _check(pg_conn) is False


def test_ingest_rejects_count_toward_ratio(pg_conn):
    _load(pg_conn, [*_fixture_records(), 1])
    _reject_silver(pg_conn)

    assert _check(pg_conn) is False


def test_silver_rejects_over_ratio_fail(pg_conn):
    valid, invalid = _fixture_records()
    invalid['lat'] = 'not a number'
    _load(pg_conn, [valid, invalid])
    _reject_silver(pg_conn)

    assert _check(pg_conn) is False


def _fixture_records() -> list:
    return json.loads(FIXTURE.read_text())


def _load(pg_conn, records: list):
    stream = io.BytesIO(json.dumps(records).encode())
    stream_to_pg_with_dlq(stream, BATCH_ID, SOURCE, pg_conn)


def _reject_silver(pg_conn):
    _execute(
        pg_conn,
        f'silver/reject_{SOURCE}.sql',
        {'batch_id': BATCH_ID},
    )


def _check(pg_conn) -> bool:
    params = {
        'source': SOURCE,
        'batch_id': BATCH_ID,
        'max_reject_ratio': MAX_REJECT_RATIO,
    }
    with pg_conn, pg_conn.cursor() as cur:
        cur.execute(
            (SQL_DIR / 'silver' / 'check_reject_ratio.sql').read_text(),
            params,
        )
        return cur.fetchone()[0]


def _execute(
    pg_conn,
    sql_path: str,
    params: dict,
):
    with pg_conn, pg_conn.cursor() as cur:
        cur.execute(
            (SQL_DIR / sql_path).read_text(),
            params,
        )

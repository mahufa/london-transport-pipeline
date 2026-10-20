import io

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg import ClientCursor, Connection

from include.connections import POSTGRES_CONN_ID
from include.datasets import RECORD_KEY_FIELDS
from include.helpers.json_validator import generate_clean_lines
from include.paths import SQL_DIR

COPY_LANDING = 'COPY landing (ordinal, record_key, payload) FROM STDIN'
COPY_REJECTED = 'COPY bronze.rejected_records (source, batch_id, record, error) FROM STDIN'


def stream_to_pg_with_dlq(
        s3_stream,
        batch_id: str,
        source: str,
        pg_conn: Connection | None = None,
):
    dlq_buffer = []
    record_keys = RECORD_KEY_FIELDS[source]
    params = {
        'source': source,
        'batch_id': batch_id,
    }
    owns_conn = pg_conn is None
    pg_conn = pg_conn or _get_pg_conn()

    try:
        with pg_conn.transaction(), ClientCursor(pg_conn) as cur:
            cur.execute(
                _read_sql('create_landing.sql'),
                params,
            )

            lines = generate_clean_lines(
                s3_stream,
                dlq_buffer,
                record_keys,
            )
            with cur.copy(COPY_LANDING) as copy:
                for line in lines:
                    copy.write(_to_copy_row(*line))

            cur.execute(
                _read_sql('merge_landing.sql'),
                params,
            )
            dlq_stream = _prepare_dlq_stream(
                dlq_buffer,
                batch_id,
                source,
            )
            with cur.copy(COPY_REJECTED) as copy:
                copy.write(dlq_stream.read())
    finally:
        if owns_conn:
            pg_conn.close()


def _prepare_dlq_stream(
        dlq_buffer: list,
        batch_id: str,
        source: str,
) -> io.StringIO:
    dlq_str = '\n'.join(
        f'{source}\t{batch_id}\t{_escape_for_copy(rec)}\t{_escape_for_copy(err_msg)}' for rec, err_msg in
        dlq_buffer)
    return io.StringIO(dlq_str)


def _read_sql(name: str) -> str:
    return (SQL_DIR / 'bronze' / name).read_text()


def _get_pg_conn() -> Connection:
    conn = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID,
    ).get_conn()
    if not isinstance(conn, Connection):
        raise TypeError(
            f'expected a psycopg 3 connection, got {type(conn).__name__}'
        )

    return conn


def _to_copy_row(
        ordinal: int,
        record_key: str,
        payload: str
) -> bytes:
    return f'{ordinal}\t{_escape_for_copy(record_key)}\t{_escape_for_copy(payload)}\n'.encode()


def _escape_for_copy(value: str) -> str:
    return (value
            .replace('\\', '\\\\')
            .replace('\t', '\\t')
            .replace('\n', '\\n')
            .replace('\r', '\\r'))

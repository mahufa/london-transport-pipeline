import io
from typing import Iterator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2._psycopg import connection

from include.connections import POSTGRES_CONN_ID
from include.datasets import RECORD_KEY_FIELDS
from include.helpers.json_validator import generate_clean_lines
from include.helpers.streams import IterStream
from include.paths import SQL_DIR

BUFFER_SIZE = 1024 * 1024
COPY_LANDING = 'COPY landing (ordinal, record_key, payload) FROM STDIN'
COPY_REJECTED = 'COPY bronze.rejected_records (source, batch_id, stage, record, error) FROM STDIN'


def stream_to_pg_with_dlq(
        s3_stream,
        batch_id: str,
        source: str,
        pg_conn: connection | None = None,
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
        with pg_conn, pg_conn.cursor() as cur:
            cur.execute(
                _read_sql('create_landing.sql'),
                params,
            )

            lines = generate_clean_lines(
                s3_stream,
                dlq_buffer,
                record_keys,
            )
            buffered_lines = _get_buffered_lines(lines)

            cur.copy_expert(COPY_LANDING, buffered_lines, size=BUFFER_SIZE)
            cur.execute(
                _read_sql('merge_landing.sql'),
                params,
            )
            dlq_stream = _prepare_dlq_stream(
                dlq_buffer,
                batch_id,
                source,
            )
            cur.copy_expert(COPY_REJECTED, dlq_stream)
    finally:
        if owns_conn:
            pg_conn.close()


def _get_buffered_lines(
        lines: Iterator[tuple[int, str, str]]
) -> io.BufferedReader:
    return io.BufferedReader(
        IterStream(_to_copy_row(*line) for line in lines),
        buffer_size=BUFFER_SIZE
    )


def _prepare_dlq_stream(
        dlq_buffer: list,
        batch_id: str,
        source: str,
) -> io.StringIO:
    dlq_str = '\n'.join(
        f'{source}\t{batch_id}\tingest\t{_escape_for_copy(rec)}\t{_escape_for_copy(err_msg)}' for rec, err_msg in
        dlq_buffer)
    return io.StringIO(dlq_str)


def _read_sql(name: str) -> str:
    return (SQL_DIR / 'bronze' / name).read_text()


def _get_pg_conn() -> connection:
    return PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID,
    ).get_conn()


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

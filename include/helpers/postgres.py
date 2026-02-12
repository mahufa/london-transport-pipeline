from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.response import StreamingBody
from psycopg2._psycopg import connection


POSTGRES_CONN_ID = 'postgres_dw'


def copy_s3_obj_to_postgres(
    s3_obj,
    table_name: str,
    batch_id: str,
):
    stream = s3_obj.get()['Body']
    conn = _get_pg_conn()

    try:
        _remove_batch_from_staging(table_name, batch_id, conn)
        _load_stream_to_pg(table_name, stream, conn)
    finally:
        conn.close()


def _remove_batch_from_staging(
    table_name: str,
    batch_id: str,
    conn: connection,
):
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table_name} WHERE batch_id='{batch_id}'")


def _load_stream_to_pg(
    table_name: str,
    stream: StreamingBody,
    conn: connection,
):
    with  conn:
        with conn.cursor() as cur:
            cur.copy_expert(f'COPY {table_name} FROM STDIN CSV HEADER', stream)



def _get_pg_conn() -> connection:
    hook = _get_pg_hook()
    return hook.get_conn()


def _get_pg_hook() -> PostgresHook:
    return PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID,
    )

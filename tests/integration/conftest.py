import os
from pathlib import Path

import psycopg2
from pytest import fixture, skip

DB_INIT = Path(__file__).parents[2] / 'db_init'


@fixture(scope='session')
def pg_conn():
    dsn = os.getenv('DW_TEST_DSN')
    if not dsn:
        skip('DW_TEST_DSN not set')

    conn = psycopg2.connect(dsn)
    with conn, conn.cursor() as cur:
        cur.execute('DROP SCHEMA IF EXISTS bronze, silver, gold CASCADE')
        for path in sorted(DB_INIT.glob('*.sql')):
            cur.execute(path.read_text())

    yield conn
    conn.close()


@fixture(autouse=True)
def clean_bronze(pg_conn):
    with pg_conn, pg_conn.cursor() as cur:
        cur.execute('TRUNCATE bronze.raw_tfl, bronze.rejected_records')

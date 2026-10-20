from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import BaseHook

from include.connections import METABASE_CONN_ID

DATABASE_NAME = 'postgres_dw'


def sync_schema() -> None:
    session = _login()
    database_id = _get_database_id(session)
    _get_metabase_hook('POST').run(
        endpoint=f'/api/database/{database_id}/sync_schema',
        headers=session,
    )


def _login() -> dict:
    conn = BaseHook.get_connection(METABASE_CONN_ID)
    response = _get_metabase_hook('POST').run(
        endpoint='/api/session',
        json={'username': conn.login, 'password': conn.password},
    )
    return {'X-Metabase-Session': response.json()['id']}


def _get_database_id(session: dict) -> int:
    response = _get_metabase_hook('GET').run(
        endpoint='/api/database',
        headers=session,
    )
    for db in response.json()['data']:
        if db['name'] == DATABASE_NAME:
            return db['id']
    raise ValueError(f"Metabase database '{DATABASE_NAME}' not found")


def _get_metabase_hook(method: str) -> HttpHook:
    return HttpHook(method=method, http_conn_id=METABASE_CONN_ID)

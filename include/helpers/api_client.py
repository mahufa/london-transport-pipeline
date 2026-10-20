from logging import getLogger

from airflow.sdk.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from requests import RequestException, Response

from include.connections import TFL_API_CONN_ID


# TODO: refactor module to look like storage.py

def get_api_data_stream(
    endpoint: str,
    params: dict = None,
) -> Response:
    api = _get_api_hook()
    response = api.run(
        endpoint=endpoint,
        data=params, #HttpHook expects GET params passed via `data=...`
        headers={'Accept-Encoding': 'gzip'},
        extra_options={'stream': True, 'timeout': (3.0, 30.0)},
    )
    return response


def is_api_available(
    endpoint: str,
    params: dict = None,
) -> bool:
    try:
        api = _get_api_hook()
        # stream and close right away, so only the status and headers are read, not the body
        api.run(
            endpoint=endpoint,
            data=params,
            extra_options={'stream': True, 'timeout': (3.0, 5.0)},
        ).close()
    except (RequestException, AirflowException):
        logger = getLogger(__name__)
        logger.exception("Failed to connect to API")
        return False
    else:
        return True


def _get_api_hook() -> HttpHook:
    return HttpHook(
        method='GET',
        http_conn_id=TFL_API_CONN_ID,
    )

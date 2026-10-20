from logging import getLogger

from airflow.sdk.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from requests import RequestException, Response

from include.connections import TFL_API_CONN_ID

logger = getLogger(__name__)


def get_api_data_stream(
    endpoint: str,
    params: dict = None,
    api: HttpHook = None,
) -> Response:
    api = api or _get_api_hook()

    return api.run(
        endpoint=endpoint,
        data=params,  # HttpHook expects GET params passed via `data=...`
        headers={'Accept-Encoding': 'gzip'},
        extra_options={'stream': True, 'timeout': (3.0, 30.0)},
    )


def is_api_available(
    endpoint: str,
    params: dict = None,
    api: HttpHook = None,
) -> bool:
    api = api or _get_api_hook()

    try:
        api.run(
            endpoint=endpoint,
            data=params,
            extra_options={'stream': True, 'timeout': (3.0, 30.0)},
        ).close()
    except (RequestException, AirflowException):
        logger.exception('Failed to connect to API')
        return False

    return True


def _get_api_hook() -> HttpHook:
    return HttpHook(
        method='GET',
        http_conn_id=TFL_API_CONN_ID,
    )

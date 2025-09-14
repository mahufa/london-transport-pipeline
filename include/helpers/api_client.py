from logging import getLogger

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


def get_api_data(
    endpoint: str,
    params: dict = None,
) -> str:
    from json import dumps
    api = _get_api_hook()
    response = api.run(
        endpoint=endpoint,
        data=params, #HttpHook expects GET params passed via `data=...`
    )
    return dumps(response.json())


def is_api_available(test_endpoint) -> bool:
    from requests import RequestException

    try:
        api = _get_api_hook()
        api.run(
            endpoint=test_endpoint,
            extra_options={'timeout': (3.0, 5.0)},
        )
    except (RequestException, AirflowException):
        logger = getLogger(__name__)
        logger.exception("Failed to connect to API")
        return False
    else:
        return True


def _get_api_hook() -> HttpHook:
    return HttpHook(
        method='GET',
        http_conn_id='tfl_api',
    )
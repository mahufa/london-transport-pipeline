from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.sensors.base import PokeReturnValue
from airflow.utils.log.logging_mixin import LoggingMixin
from requests import RequestException


def _check_api():
    api = HttpHook(
        method='GET',
        http_conn_id='tfl_api',
    )
    try:
        api.run(
            endpoint='Line/Mode/tube/Status',
            extra_options = {"timeout": (3.0, 5.0)},
        )
    except (RequestException, AirflowException):
        LoggingMixin().log.exception('Failed to connect the API')
        condition = False
    else:
        condition = True

    return PokeReturnValue(is_done=condition)

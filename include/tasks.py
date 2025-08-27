from airflow.exceptions import AirflowException
from airflow.sensors.base import PokeReturnValue
from airflow.utils.log.logging_mixin import LoggingMixin

from include.helpers.api_client import get_api_hook


def _check_api() -> PokeReturnValue:
    from requests import RequestException
    api = get_api_hook()
    try:
        api.run(
            endpoint='/Line/Meta/Modes',
            extra_options = {'timeout': (3.0, 5.0)},
        )
    except (RequestException, AirflowException):
        LoggingMixin().log.exception('Failed to connect the API')
        condition = False
    else:
        condition = True

    return PokeReturnValue(is_done=condition)

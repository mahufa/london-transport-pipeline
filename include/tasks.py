from typing import Callable

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.sensors.base import PokeReturnValue
from airflow.utils.log.logging_mixin import LoggingMixin

from include.helpers.api_client import get_api_hook


def make_check_api_task(
    poke_interval=30,
    timeout=300,
    mode="reschedule"
) -> Callable:
    @task.sensor(
        poke_interval=poke_interval,
        timeout=timeout,
        mode=mode
    )
    def _check_api_task() -> PokeReturnValue:
        return _check_api()

    return _check_api_task

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

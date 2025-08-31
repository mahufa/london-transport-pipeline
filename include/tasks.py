from typing import Callable

from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue

from include.helpers.api_client import is_api_available


def make_check_api_task(
    poke_interval=10,
    timeout=100,
    mode='poke',
) -> Callable:
    @task.sensor(
        poke_interval=poke_interval,
        timeout=timeout,
        mode=mode
    )
    def _check_api_task() -> PokeReturnValue:
        condition = is_api_available()
        return PokeReturnValue(is_done=condition)

    return _check_api_task

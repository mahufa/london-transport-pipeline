from typing import Callable

from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue

from include.helpers.api_client import get_api_hook, is_api_available


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
    api = get_api_hook()
    condition = is_api_available(api)

    return PokeReturnValue(is_done=condition)
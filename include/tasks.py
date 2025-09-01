from typing import Callable

from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue


def make_get_data_task(
        task_id: str,
        endpoint: str,
        templated_params: dict = None,
) -> Callable:

    @task(
        task_id=task_id,
        templates_dict=templated_params
    )
    def _get_data_task(templates_dict) -> str:
        from include.helpers.api_client import get_api_data

        return get_api_data(endpoint, templates_dict)

    return _get_data_task


def make_check_api_sensor(
    test_endpoint: str = '/Line/Meta/Modes',
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
        from include.helpers.api_client import is_api_available

        condition = is_api_available(test_endpoint)
        return PokeReturnValue(is_done=condition)

    return _check_api_task

from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue


def make_emit_dataset_task(
    dataset: Dataset,
    upstream_task_id: str = '_store_data_task',
) -> Callable:

    @task(
        templates_dict={
            'path': f'{{{{ ti.xcom_pull(task_ids="{upstream_task_id}") }}}}'
        },
        outlets=[dataset],
    )
    def _emit_dataset_task(templates_dict):
        from airflow.datasets.metadata import Metadata
        from include.datasets import PATH_KEY

        yield Metadata(
            target=dataset,
            extra={PATH_KEY: templates_dict['path']}
        )

    return _emit_dataset_task


def make_store_data_task(
    dir_name: str,
    upstream_task_id: str = '_get_data_task',
) -> Callable:

    @task(
        templates_dict={
            'data': f'{{{{ ti.xcom_pull(task_ids="{upstream_task_id}") }}}}',
            'path': f'{dir_name}{{{{ ds }}}}/{{{{ ts_nodash }}}}.json'
        },
    )
    def _store_data_task(templates_dict) -> str:
        from include.helpers.storage import store_str_in_s3

        data = templates_dict['data']
        path = templates_dict['path']
        store_str_in_s3(data, path)
        return path

    return _store_data_task


def make_get_data_task(
        endpoint: str,
        templated_params: dict = None,
) -> Callable:

    @task(
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

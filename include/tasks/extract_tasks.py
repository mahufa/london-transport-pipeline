from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue


def make_emit_dataset_task(
    dataset: Dataset,
    upstream_task_id: str = '_ingest_data_task',
) -> Callable:

    @task(outlets=[dataset])
    def _emit_dataset_task(ti):
        from airflow.datasets.metadata import Metadata
        from include.datasets import PATH_KEY

        yield Metadata(
            target=dataset,
            extra={PATH_KEY: ti.xcom_pull(task_ids=upstream_task_id)},
        )

    return _emit_dataset_task


def make_ingest_data_task(
        endpoint: str,
        dir_name: str,
        templated_params: dict = None,
) -> Callable:
    templates = (
        {param_name : param for param_name, param in templated_params.items()}
        if templated_params else {}
    )
    templates['path'] = f'{dir_name}{{{{ ds }}}}/{{{{ ts_nodash }}}}.json'

    @task(templates_dict=templates)
    def _ingest_data_task(templates_dict) -> str:
        from include.helpers.api_client import get_api_data
        from include.helpers.storage import store_str_in_s3

        path = templates_dict.pop('path')
        data = get_api_data(endpoint, templates_dict)
        store_str_in_s3(data, path)
        return path

    return _ingest_data_task


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

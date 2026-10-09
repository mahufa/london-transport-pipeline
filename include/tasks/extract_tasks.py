from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.sensors.base import PokeReturnValue

from include.datasets import EXTRA_VAL_KEYS


def make_ingest_data_task(
        endpoint: str,
        dataset: Dataset,
        templated_params: dict = None,
) -> Callable:
    templates = (
        {param_name : param for param_name, param in templated_params.items()}
        if templated_params else {}
    )
    templates['path'] = f'{dataset.uri}{{{{ ds }}}}/{{{{ ts_nodash }}}}.json'

    @task(templates_dict=templates, outlets=[dataset])
    def _ingest_data_task(templates_dict, *, outlet_events=None) -> None:
        from include.helpers.api_client import get_api_data_stream
        from include.helpers.storage import store_stream_in_s3

        path = templates_dict.pop('path')
        with get_api_data_stream(endpoint, templates_dict) as api_response:
            if 'gzip' in api_response.headers.get('Content-Encoding', '').lower():
                path += '.gz'

            store_stream_in_s3(api_response.raw, path)

        outlet_events[dataset].extra = {EXTRA_VAL_KEYS['raw']: path}

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

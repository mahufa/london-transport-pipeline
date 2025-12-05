from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task, task_group

from include.datasets import LayerDatasets
from include.helpers.dataset_utils import get_dataset_short_name, get_batch_id_from_path, get_path_to_staging
from include.tasks.common_tasks import make_emit_dataset_task


def build_raw_dataset_flow(layer_datasets: LayerDatasets) -> Callable:

    @task_group(
        group_id=f'process__{get_dataset_short_name(layer_datasets.raw.uri)}'
    )
    def _process_raw_dataset(paths: list[str]):
        prepare_data = _make_prepare_data_task(layer_datasets.raw)
        emit_data = make_emit_dataset_task(layer_datasets.staging)

        prepared = prepare_data.expand(path_to_raw=paths)
        emit_data.expand(path=prepared)


    return _process_raw_dataset


def _make_prepare_data_task(
    raw_dataset: Dataset
) -> Callable:

    @task(
        task_id=f'prepare__{get_dataset_short_name(raw_dataset.uri)}',
    )
    def _prepare_data(path_to_raw: str) -> str:
        from include.cleaners import clean_dataset
        from include.helpers.storage import read_str_from_s3, store_str_in_s3

        raw_data = read_str_from_s3(path_to_raw)

        path_to_staging = get_path_to_staging(path_to_raw)
        batch_id = get_batch_id_from_path(path_to_staging)

        transformed_csv = clean_dataset(raw_dataset.uri, raw_data, batch_id)

        store_str_in_s3(transformed_csv, path_to_staging)

        return path_to_staging

    return _prepare_data

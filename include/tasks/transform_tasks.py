from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task, task_group

from include.helpers.dataset_utils import get_dataset_short_name



def build_dataset_flow(
    dataset: Dataset,
    upstream_task_id: str = '_get_paths_to_raw',
):

    @task_group(
        group_id=f'process__{get_dataset_short_name(dataset.uri)}'
    )
    def _process_dataset():
        paths = _make_extract_dataset_paths_task(dataset, upstream_task_id)
        fetch_data = _make_fetch_stored_raw_task(dataset)
        prepare_data = _make_prepare_data_task(dataset)

        fetched = fetch_data.expand(path=paths())
        prepare_data.expand(raw_data=fetched)

    return _process_dataset


def make_get_paths_to_raw_task() -> Callable:

    @task
    def _get_paths_to_raw(triggering_dataset_events) -> dict[str, list[str]]:
        from include.helpers.dataset_utils import get_events_paths

        return get_events_paths(triggering_dataset_events)

    return _get_paths_to_raw


def _make_prepare_data_task(
    dataset: Dataset
) -> Callable:

    @task(
        task_id=f'prepare__{get_dataset_short_name(dataset.uri)}',
    )
    def _prepare_data(raw_data: str) -> bytes:
        from include.transformers import DatasetTransformer

        transformer = DatasetTransformer.for_dataset(dataset.uri)
        return transformer.prepare_to_load(raw_data)

    return _prepare_data


def _make_fetch_stored_raw_task(
    dataset: Dataset
) -> Callable:

    @task(
        task_id=f'fetch_stored__{get_dataset_short_name(dataset.uri)}',
    )
    def _fetch_stored_raw(path: str) -> str:
        from include.helpers.storage import read_str_from_s3

        return read_str_from_s3(path)

    return _fetch_stored_raw


def _make_extract_dataset_paths_task(
    dataset: Dataset,
    upstream_task_id,
) -> Callable:

    @task(
        task_id=f'extract_paths_to__{get_dataset_short_name(dataset.uri)}',
    )
    def _extract_dataset_paths(ti=None) -> list[str]:
        all_paths = ti.xcom_pull(task_ids=upstream_task_id)
        specific_dataset_paths = all_paths.get(dataset.uri, [])
        return specific_dataset_paths

    return _extract_dataset_paths

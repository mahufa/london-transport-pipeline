from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task

from include.helpers.dataset_utils import get_dataset_short_name





def make_fetch_stored_raw_task(
    dataset: Dataset
) -> Callable:
    @task(
        task_id=f'fetch_stored__{get_dataset_short_name(dataset.uri)}',
    )
    def _fetch_stored_raw(path: str) -> str:
        from include.helpers.storage import read_str_from_s3

        return read_str_from_s3(path)

    return _fetch_stored_raw


def make_extract_dataset_paths_task(
    dataset: Dataset,
    upstream_task_id: str = '_get_paths_to_raw'
) -> Callable:

    @task(
        task_id=f'extract_paths_to__{get_dataset_short_name(dataset.uri)}',
    )
    def _extract_dataset_paths(ti=None) -> list[str]:
        all_paths = ti.xcom_pull(task_ids=upstream_task_id)
        specific_dataset_paths = all_paths.get(dataset.uri, [])
        return specific_dataset_paths

    return _extract_dataset_paths


def make_get_paths_to_raw_task() -> Callable:

    @task
    def _get_paths_to_raw(triggering_dataset_events) -> dict[str, list[str]]:
        from include.helpers.dataset_utils import get_events_paths

        return get_events_paths(triggering_dataset_events)

    return _get_paths_to_raw
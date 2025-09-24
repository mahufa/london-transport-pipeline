from typing import Callable

from airflow.datasets import Dataset
from airflow.datasets.metadata import Metadata
from airflow.decorators import task

from include.datasets import PATH_KEY
from include.helpers.dataset_utils import get_dataset_short_name


def make_extract_dataset_paths_task(dataset: Dataset) -> Callable:

    @task(
        task_id=f'extract_paths_to__{get_dataset_short_name(dataset.uri)}',
    )
    def _extract_dataset_paths(all_paths: dict[str, list[str]]) -> list[str]:
        specific_dataset_paths = all_paths.get(dataset.uri, [])
        return specific_dataset_paths

    return _extract_dataset_paths


def make_get_paths_to_triggering_data_task() -> Callable:

    @task
    def _get_paths_to_triggering_data(triggering_dataset_events) -> dict[str, list[str]]:
        from include.helpers.dataset_utils import get_events_paths

        return get_events_paths(triggering_dataset_events)

    return _get_paths_to_triggering_data


def make_emit_dataset_task(dataset: Dataset) -> Callable:

    @task(outlets=[dataset])
    def _emit_dataset_task(path: str):
        yield Metadata(
            target=dataset,
            extra={PATH_KEY: path},
        )

    return _emit_dataset_task
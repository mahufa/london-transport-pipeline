from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task, task_group

from include.helpers.dataset_utils import get_dataset_short_name



def build_dataset_flow(dataset: Dataset) -> Callable:

    @task_group(
        group_id=f'process__{get_dataset_short_name(dataset.uri)}'
    )
    def _process_dataset(paths: list[str]):
        fetch_raw = _make_fetch_stored_raw_task(dataset)
        prepare_data = _make_prepare_data_task(dataset)

        fetched = fetch_raw.expand(path=paths)
        prepare_data.expand(raw_data=fetched)

    return _process_dataset


def make_extract_dataset_paths_task(
    dataset: Dataset,
) -> Callable:

    @task(
        task_id=f'extract_paths_to__{get_dataset_short_name(dataset.uri)}',
    )
    def _extract_dataset_paths(all_paths: dict[str, list[str]]) -> list[str]:
        specific_dataset_paths = all_paths.get(dataset.uri, [])
        return specific_dataset_paths

    return _extract_dataset_paths


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
    def _prepare_data(path_to_raw: str) -> str:
        from include.transformers import DatasetTransformer
        from include.helpers.storage import read_str_from_s3, store_str_in_s3
        from include.helpers.dataset_utils import get_path_to_staging

        transformer = DatasetTransformer.for_dataset(dataset.uri)
        raw_data = read_str_from_s3(path_to_raw)

        transformed_csv = transformer.prepare_to_load(raw_data)
        path_to_staging = get_path_to_staging(path_to_raw)

        store_str_in_s3(transformed_csv, path_to_staging)

        return path_to_staging

    return _prepare_data

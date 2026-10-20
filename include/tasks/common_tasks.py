from typing import Callable

from airflow.sdk import Asset, task

from include.datasets import EXTRA_VAL_KEYS
from include.helpers.dataset_utils import get_dataset_short_name, get_layer_from_uri


def make_extract_dataset_extras_task(dataset: Asset) -> Callable:

    @task(
        task_id=f'extract_{EXTRA_VAL_KEYS[get_layer_from_uri(dataset.uri)]}s_to__{get_dataset_short_name(dataset.uri)}',
    )
    def _extract_dataset_extras(all_extras: dict[str, list[str]]) -> list[str]:
        specific_dataset_extras = all_extras.get(dataset.uri, [])
        return specific_dataset_extras

    return _extract_dataset_extras


def make_get_extras_from_triggering_data_task() -> Callable:

    @task
    def _get_extras_from_triggering_data(triggering_asset_events) -> dict[str, list[str]]:
        from include.helpers.dataset_utils import get_event_extras

        return get_event_extras(triggering_asset_events)

    return _get_extras_from_triggering_data

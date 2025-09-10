from airflow.models.dataset import DatasetEvent

from include.datasets import PATH_KEY


def get_dataset_short_name(dataset_uri: str) -> str:
    return dataset_uri.split(sep="/")[0]


def get_events_paths(triggering_dataset_events: dict) -> dict[str, list[str]]:
    paths_grouped_by_datasets = {
        dataset_uri: _get_paths_from_events_list(event_list)
        for dataset_uri, event_list in triggering_dataset_events.items()
    }
    return paths_grouped_by_datasets


def _get_paths_from_events_list(event_list: list[DatasetEvent]) -> list[str]:
    paths = [_get_path_from_event(event) for event in event_list]
    return paths


def _get_path_from_event(event: DatasetEvent) -> str:
    path = event.extra.get(PATH_KEY)
    if path is None:
        raise KeyError(
            f'Path not found in metadata for dataset {event.uri}'
        )
    return path

from airflow.models.dataset import DatasetEvent

from include.datasets import EXTRA_VAL_KEYS


def get_batch_id_from_path(path: str):
    return (path
            .replace('.json', '')
            .replace('.gz', '')
            )[-13:]


def get_dataset_short_name(dataset_uri: str) -> str:
    return dataset_uri.split(sep="/")[0]


def get_event_extras(triggering_dataset_events: dict) -> dict[str, list[str]]:
    extras_grouped_by_datasets = {
        dataset_uri: _get_extras_from_events_list(event_list, dataset_uri)
        for dataset_uri, event_list in triggering_dataset_events.items()
    }
    return extras_grouped_by_datasets


def get_layer_from_uri(uri: str):
    return uri.rstrip('/').split(sep='/')[-1]


def _get_extras_from_events_list(
    event_list: list[DatasetEvent],
    dataset_uri: str,
) -> list[str]:
    extras = [_get_extra_val_from_event(event, dataset_uri) for event in event_list]
    return extras


def _get_extra_val_from_event(
    event: DatasetEvent,
    dataset_uri: str
) -> str:
    extra_val_key = EXTRA_VAL_KEYS[get_layer_from_uri(dataset_uri)]
    extra_val = event.extra.get(extra_val_key)
    if extra_val is None:
        raise KeyError(
            f'Extra value not found in metadata for dataset {event.uri}'
        )
    return extra_val

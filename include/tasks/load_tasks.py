from typing import Callable

from airflow import XComArg
from airflow.decorators import task, task_group

from include.datasets import EXTRA_VAL_KEYS, LayerDatasets
from include.helpers.dataset_utils import get_dataset_short_name, get_batch_id_from_path


def build_raw_dataset_flow(layer_datasets: LayerDatasets) -> Callable:

    @task_group(
        group_id=f'process__{get_dataset_short_name(layer_datasets.raw.uri)}'
    )
    def _process_raw_dataset(paths: XComArg):
        prepare_data = _make_validate_and_load_task(layer_datasets)

        prepare_data.expand(path_to_raw=paths)

    return _process_raw_dataset


def _make_validate_and_load_task(
    layer_datasets: LayerDatasets
) -> Callable:
    source = get_dataset_short_name(layer_datasets.raw.uri)
    bronze = layer_datasets.bronze

    @task(
        task_id=f'validate_and_load__{source}',
        outlets=[bronze],
    )
    def _validate_and_load(path_to_raw: str, *, outlet_events=None) -> None:
        from include.helpers.storage import get_s3_obj
        from include.helpers.postgres import stream_to_pg_with_dlq
        import gzip

        batch_id = get_batch_id_from_path(path_to_raw)
        raw_obj = get_s3_obj(path_to_raw)
        body = raw_obj['Body']

        with (
            gzip.GzipFile(fileobj=body)
            if path_to_raw.endswith('.gz')
            else body
            as raw_stream
        ):
            stream_to_pg_with_dlq(
                raw_stream,
                batch_id,
                source,
            )

        outlet_events[bronze].extra = {EXTRA_VAL_KEYS['bronze']: batch_id}

    return _validate_and_load

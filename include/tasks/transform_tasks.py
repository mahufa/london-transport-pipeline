from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task, task_group

from include.datasets import LayerDatasets
from include.helpers.dataset_utils import get_dataset_short_name, get_batch_id_from_path
from include.tasks.common_tasks import make_emit_dataset_task


def build_raw_dataset_flow(layer_datasets: LayerDatasets) -> Callable:

    @task_group(
        group_id=f'process__{get_dataset_short_name(layer_datasets.raw.uri)}'
    )
    def _process_raw_dataset(paths: list[str]):
        prepare_data = _make_validate_and_load_task(layer_datasets.raw)
        emit_data = make_emit_dataset_task(layer_datasets.bronze)

        prepared = prepare_data.expand(path_to_raw=paths)
        emit_data.expand(path=prepared)


    return _process_raw_dataset


def _make_validate_and_load_task(
    raw_dataset: Dataset
) -> Callable:
    source = get_dataset_short_name(raw_dataset.uri)

    @task(
        task_id=f'validate_and_load__{source}',
    )
    def _validate_and_load(path_to_raw: str) -> tuple[str,str]:
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

        return source, batch_id

    return _validate_and_load

from typing import Callable

from airflow.datasets import Dataset
from airflow.decorators import task


def make_emit_dataset_task(
    dataset: Dataset,
) -> Callable:

    @task(outlets=[dataset])
    def _emit_dataset_task(path: str):
        from airflow.datasets.metadata import Metadata
        from include.datasets import PATH_KEY

        yield Metadata(
            target=dataset,
            extra={PATH_KEY: path},
        )

    return _emit_dataset_task
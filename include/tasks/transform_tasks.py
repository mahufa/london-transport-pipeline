from typing import Callable

from airflow import Dataset
from airflow.decorators import task_group, task
from airflow.models.mappedoperator import OperatorPartial
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.datasets import LayerDatasets
from include.helpers.dataset_utils import get_dataset_short_name, get_batch_id_from_path


def build_bronze_dataset_flow(layer_datasets: LayerDatasets) -> Callable:
    @task_group(
        group_id=f'transform__{get_dataset_short_name(layer_datasets.bronze.uri)}'
    )
    def _process_bronze_dataset(batch_ids: list[str]):
        pass

    return _process_bronze_dataset

from typing import Callable

from airflow import Dataset
from airflow.decorators import task_group, task
from airflow.models.mappedoperator import OperatorPartial
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from include.datasets import LayerDatasets
from include.helpers.dataset_utils import get_dataset_short_name, get_batch_id_from_path


def build_staging_dataset_flow(layer_datasets: LayerDatasets) -> Callable:
    @task_group(
        group_id=f'load__{get_dataset_short_name(layer_datasets.staging.uri)}'
    )
    def _process_staging_dataset(paths: list[str]):
        copy_task = _make_copy_csv_from_s3_to_staging_table_task(layer_datasets.staging)
        merge_op = _make_merge_to_star_schema_operator(layer_datasets.staging)

        copy_results = copy_task.expand(path_to_staging=paths)
        merge_op.expand(parameters=copy_results)

    return _process_staging_dataset


def _make_merge_to_star_schema_operator(staging_dataset: Dataset) -> OperatorPartial:
    from include.helpers.postgres import POSTGRES_CONN_ID

    dataset_short_name = get_dataset_short_name(staging_dataset.uri)

    return SQLExecuteQueryOperator.partial(
        task_id=f'merge__{dataset_short_name}',
        conn_id=POSTGRES_CONN_ID,
        sql = f'sql/merge_{dataset_short_name}.sql',
    )


def _make_copy_csv_from_s3_to_staging_table_task(staging_dataset: Dataset) -> Callable:
    dataset_short_name = get_dataset_short_name(staging_dataset.uri)

    @task(
        task_id=f'copy__{dataset_short_name}',
        multiple_outputs=False,
    )
    def _copy_csv_from_s3_to_staging_table(path_to_staging: str):
        from include.helpers.storage import get_s3_obj
        from include.helpers.postgres import copy_s3_obj_to_postgres

        obj = get_s3_obj(path_to_staging)
        table_name = f'staging_{dataset_short_name}'
        batch_id = get_batch_id_from_path(path_to_staging)

        copy_s3_obj_to_postgres(obj, table_name, batch_id)

        return {'batch_id': batch_id}

    return _copy_csv_from_s3_to_staging_table



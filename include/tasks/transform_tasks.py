from typing import Callable

from airflow import XComArg
from airflow.decorators import task_group
from airflow.models.mappedoperator import OperatorPartial
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

from include.dag_config import MAX_REJECT_RATIO
from include.datasets import LayerDatasets
from include.helpers.dataset_utils import get_dataset_short_name


def build_bronze_dataset_flow(layer_datasets: LayerDatasets) -> Callable:
    source = get_dataset_short_name(layer_datasets.bronze.uri)

    @task_group(
        group_id=f'transform__{source}'
    )
    def _process_bronze_dataset(batch_ids: XComArg):
        params = batch_ids.map(
            lambda batch_id: {'batch_id': batch_id}
        )
        check_params = batch_ids.map(
            lambda batch_id: {
                'batch_id': batch_id,
                'source': source,
                'max_reject_ratio': MAX_REJECT_RATIO,
            }
        )

        reject_op = _make_reject_invalid_operator(source)
        check_op = _make_check_batch_reject_ratio_operator(source)
        merge_op = _make_merge_to_star_schema_operator(source)

        reject = reject_op.expand(parameters=params)
        check = check_op.expand(parameters=check_params)
        merge = merge_op.expand(parameters=params)

        reject >> check >> merge

    return _process_bronze_dataset


def _make_merge_to_star_schema_operator(dataset_short_name: str) -> OperatorPartial:
    from include.helpers.postgres import POSTGRES_CONN_ID

    return SQLExecuteQueryOperator.partial(
        task_id=f'merge__{dataset_short_name}',
        conn_id=POSTGRES_CONN_ID,
        sql=f'sql/gold/merge_{dataset_short_name}.sql',
        max_active_tis_per_dagrun=1,
    )


def _make_check_batch_reject_ratio_operator(dataset_short_name: str) -> OperatorPartial:
    from include.helpers.postgres import POSTGRES_CONN_ID

    return SQLCheckOperator.partial(
        task_id=f'check_reject_ratio_of__{dataset_short_name}',
        conn_id=POSTGRES_CONN_ID,
        sql=f'sql/silver/check_reject_ratio.sql',
    )


def _make_reject_invalid_operator(dataset_short_name: str) -> OperatorPartial:
    from include.helpers.postgres import POSTGRES_CONN_ID

    return SQLExecuteQueryOperator.partial(
        task_id=f'reject__{dataset_short_name}',
        conn_id=POSTGRES_CONN_ID,
        sql=f'sql/silver/reject_{dataset_short_name}.sql',
    )

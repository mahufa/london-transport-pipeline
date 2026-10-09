from typing import Callable

from airflow.decorators import task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

from include.connections import POSTGRES_CONN_ID
from include.dag_config import MAX_REJECT_RATIO
from include.datasets import LayerDatasets
from include.helpers.dataset_utils import get_dataset_short_name


def build_bronze_dataset_flow(layer_datasets: LayerDatasets) -> Callable:
    source = get_dataset_short_name(layer_datasets.bronze.uri)

    @task_group(
        group_id=f'transform__{source}'
    )
    def _process_bronze_dataset(batch_id: str):
        params = {'batch_id': batch_id}
        check_params = (
                params
                |
                {
                    'source': source,
                    'max_reject_ratio': MAX_REJECT_RATIO
                }
        )

        reject = _make_reject_invalid_op(source, params)
        check = _make_check_batch_reject_ratio_op(source, check_params)
        merge = _make_merge_to_star_schema_op(source, params)

        reject >> check >> merge

    return _process_bronze_dataset


def _make_merge_to_star_schema_op(
    dataset_short_name: str,
    params: dict,
) -> SQLExecuteQueryOperator:
    return SQLExecuteQueryOperator(
        task_id=f'merge__{dataset_short_name}',
        conn_id=POSTGRES_CONN_ID,
        sql=f'sql/gold/merge_{dataset_short_name}.sql',
        max_active_tis_per_dagrun=1,
        parameters=params,
    )


def _make_check_batch_reject_ratio_op(
    dataset_short_name: str,
    params: dict,
) -> SQLCheckOperator:
    return _SQLCheckOperator(
        task_id=f'check_reject_ratio_of__{dataset_short_name}',
        conn_id=POSTGRES_CONN_ID,
        sql='sql/silver/check_reject_ratio.sql',
        parameters=params,
    )


def _make_reject_invalid_op(
    dataset_short_name: str,
    params: dict,
) -> SQLExecuteQueryOperator:
    return SQLExecuteQueryOperator(
        task_id=f'reject__{dataset_short_name}',
        conn_id=POSTGRES_CONN_ID,
        sql=f'sql/silver/reject_{dataset_short_name}.sql',
        parameters=params,
    )


# SQLCheckOperator doesn't template `parameters`, so the mapped batch_id wouldn't be rendered
class _SQLCheckOperator(SQLCheckOperator):
    template_fields = (*SQLCheckOperator.template_fields, 'parameters')
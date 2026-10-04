from airflow.decorators import dag

from include.callbacks import notify_teams
from include.dag_config import START_DATE, DEFAULT_RETRIES, DEFAULT_DAGRUN_TIMEOUT
from include.datasets import DATASETS
from include.paths import INCLUDE_DIR
from include.tasks.common_tasks import make_get_extras_from_triggering_data_task, make_extract_dataset_extras_task
from include.tasks.transform_tasks import build_bronze_dataset_flow


@dag(
    dag_id='transformer',
    start_date=START_DATE,
    schedule=(
            DATASETS.get('bike_points').bronze
            | DATASETS.get('chargers').bronze
            | DATASETS.get('roads').bronze
    ),
    catchup=False,
    description=f'This DAG checks and transforms tfl data',
    tags=['tfl', 'transform'],
    default_args={
        'retries': DEFAULT_RETRIES,
        'on_failure_callback': notify_teams,
    },
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    max_consecutive_failed_dag_runs=2,
    template_searchpath=[str(INCLUDE_DIR)]
)
def transform():
    all_bronze_batch_ids = make_get_extras_from_triggering_data_task()()

    for layer_datasets in DATASETS.values():
        extract_dataset_batch_ids = make_extract_dataset_extras_task(layer_datasets.bronze)
        transform_dataset = build_bronze_dataset_flow(layer_datasets)

        transform_dataset(
            batch_ids=extract_dataset_batch_ids(all_bronze_batch_ids)
        )


transform()

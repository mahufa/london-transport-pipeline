from airflow.decorators import dag

from include.callbacks import notify_teams
from include.dag_config import START_DATE, DEFAULT_RETRIES, DEFAULT_DAGRUN_TIMEOUT
from include.datasets import DATASETS
from include.paths import INCLUDE_DIR
from include.tasks.common_tasks import make_get_paths_to_triggering_data_task, make_extract_dataset_paths_task
from include.tasks.load_tasks import build_staging_dataset_flow


@dag(
    dag_id='loader',
    start_date=START_DATE,
    schedule=(
            DATASETS.get('bike_points').staging
            | DATASETS.get('chargers').staging
            | DATASETS.get('roads').staging
    ),
    catchup=False,
    description=f'This DAG loads and models tfl data',
    tags=['tfl', 'load'],
    default_args={
        'retries': DEFAULT_RETRIES,
        'on_failure_callback': notify_teams,
    },
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    max_consecutive_failed_dag_runs=2,
    template_searchpath=[str(INCLUDE_DIR)]
)
def load():
    all_staging_paths = make_get_paths_to_triggering_data_task()()

    for layer_datasets in DATASETS.values():
        extract_dataset_paths = make_extract_dataset_paths_task(layer_datasets.staging)
        load_dataset = build_staging_dataset_flow(layer_datasets)

        load_dataset(
            paths=extract_dataset_paths(all_staging_paths)
        )
load()

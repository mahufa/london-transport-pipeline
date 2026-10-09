from airflow.decorators import dag

from include.callbacks import notify_teams
from include.dag_config import START_DATE, DEFAULT_DAGRUN_TIMEOUT, DEFAULT_RETRIES
from include.datasets import DATASETS
from include.tasks.common_tasks import make_get_extras_from_triggering_data_task, make_extract_dataset_extras_task
from include.tasks.load_tasks import build_raw_dataset_flow


@dag(
    dag_id='loader',
    start_date=START_DATE,
    schedule=(
            DATASETS.get('bike_points').raw
            | DATASETS.get('chargers').raw
            | DATASETS.get('roads').raw
    ),
    catchup=False,
    description=f'This DAG loads tfl data',
    tags=['tfl', 'load'],
    default_args={
        'retries': DEFAULT_RETRIES,
        'on_failure_callback': notify_teams,
    },
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
)
def load():
    all_raw_paths = make_get_extras_from_triggering_data_task()()

    for layer_datasets in DATASETS.values():
        extract_dataset_paths = make_extract_dataset_extras_task(layer_datasets.raw)
        process_dataset = build_raw_dataset_flow(layer_datasets)

        process_dataset(
            paths=extract_dataset_paths(all_raw_paths)
        )


load()

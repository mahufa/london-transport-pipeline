from airflow.decorators import dag
from pendulum import duration

from include.callbacks import notify_teams
from include.dag_config import START_DATE
from include.datasets import DATASET_E_BIKES, DATASET_E_CHARGERS, DATASET_E_ROADS, EXTRACT_DATASETS
from include.tasks.transform_tasks import make_get_paths_to_raw_task, build_dataset_flow,make_extract_dataset_paths_task


# TODO:
#  tasks:
#   - branch by dataset:
#       - fetch from s3
#       - clean
#       - transform to parquet
#       - store to s3
#       - emit dataset


@dag(
    dag_id='transformer',
    start_date=START_DATE,
    schedule=(DATASET_E_BIKES | DATASET_E_CHARGERS | DATASET_E_ROADS),
    catchup=False,
    description=f'This DAG transforms tfl data',
    tags=['tfl', 'transform'],
    default_args={
        'retries': 2,
        'on_failure_callback': notify_teams,
    },
    dagrun_timeout=duration(minutes=10),
    max_consecutive_failed_dag_runs=2,
)
def transform():
    all_paths = make_get_paths_to_raw_task()()

    for dataset in EXTRACT_DATASETS:
        extract_dataset_paths = make_extract_dataset_paths_task(dataset)
        process_dataset = build_dataset_flow(dataset)

        process_dataset(
            paths=extract_dataset_paths(all_paths)
        )


transform()
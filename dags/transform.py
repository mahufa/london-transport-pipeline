from airflow.decorators import dag
from pendulum import duration

from include.callbacks import notify_teams
from include.dag_config import START_DATE
from include.datasets import DATASET_BIKES, DATASET_CHARGERS, DATASET_ROADS
from include.tasks.transform_tasks import make_get_paths_to_raw_task


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
    schedule=(DATASET_BIKES | DATASET_CHARGERS | DATASET_ROADS),
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
    get_paths_to_raw = make_get_paths_to_raw_task()

    get_paths_to_raw()


transform()
from airflow.decorators import dag
from pendulum import duration

from include.callbacks import notify_teams
from include.dag_config import START_DATE
from include.datasets import DATASET_BIKES, DATASET_CHARGERS, DATASET_ROADS


# TODO:
#  tasks:
#  - fetch from s3
#  - branch by dataset:
#       - clean
#       - transform to parquet
#  - store to s3


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
    pass


transform()
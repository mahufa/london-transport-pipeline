from airflow.decorators import dag, task

from pendulum import duration, datetime, UTC

from include.helpers.api_client import get_api_data
from include.tasks import make_check_api_task


@dag(
    start_date=datetime(2025, 6, 1).astimezone(UTC),
    schedule="*/30 * * * *",
    catchup=False,
    description='This DAG extracts chargers data from tfl',
    tags=[
        'tfl',
        'extract',
        'chargers'
    ],
    default_args={
        'retries': 2,
    },
    dagrun_timeout=duration(minutes=10),
    max_consecutive_failed_dag_runs=2,
)
def tfl_chargers():

    check_api = make_check_api_task()

    @task()
    def get_tfl_chargers():
        endpoint = '/Place/Type/ChargeConnector'
        return  get_api_data(endpoint)


    check_api() >> get_tfl_chargers()


tfl_chargers()
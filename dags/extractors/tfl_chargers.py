from airflow.decorators import dag, task

from pendulum import duration, datetime, UTC

from include.tasks import make_check_api_task, make_get_data_task


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

    get_tfl_chargers = make_get_data_task(
        task_id='get_tfl_chargers',
        endpoint='/Place/Type/ChargeConnector'
    )

    check_api() >> get_tfl_chargers()


tfl_chargers()
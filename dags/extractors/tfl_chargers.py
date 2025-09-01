from airflow.decorators import dag

from pendulum import duration, datetime, UTC

from include.tasks import make_check_api_sensor, make_get_data_task, make_store_data_task


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

    check_api = make_check_api_sensor()

    get_tfl_chargers = make_get_data_task(
        endpoint='/Place/Type/ChargeConnector'
    )

    store_chargers = make_store_data_task(
        dir_name='chargers'
    )

    check_api() >> get_tfl_chargers() >> store_chargers()


tfl_chargers()
from airflow.decorators import dag

from pendulum import duration, datetime, UTC

from include.datasets import DATASET_BIKES
from include.tasks import make_check_api_sensor, make_get_data_task, make_store_data_task


@dag(
    start_date=datetime(2025, 6, 1).astimezone(UTC),
    schedule="*/30 * * * *",
    catchup=False,
    description='This DAG extracts bike points data from tfl',
    tags=[
        'tfl',
        'extract',
        'bikes'
    ],
    default_args={
        'retries': 2,
    },
    dagrun_timeout=duration(minutes=10),
    max_consecutive_failed_dag_runs=2,
)
def tfl_bikes():

    check_api = make_check_api_sensor()

    get_tfl_bike_points = make_get_data_task(
        endpoint='/Place/Type/ChargeConnector'
    )

    store_bike_points = make_store_data_task(
        dir_name='bike-points',
        dataset=DATASET_BIKES,
    )

    check_api() >> get_tfl_bike_points() >> store_bike_points()

tfl_bikes()
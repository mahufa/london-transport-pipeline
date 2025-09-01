from airflow.decorators import dag, task

from pendulum import duration, datetime

from include.helpers.api_client import get_api_data
from include.tasks import make_check_api_task


@dag(
    start_date=datetime(2025, 6, 1),
    schedule='@daily',
    catchup=False,
    description='This DAG extracts road disruptions data from tfl',
    tags=[
        'tfl',
        'extract',
        'roads'
    ],
    default_args={
        'retries': 2,
    },
    dagrun_timeout=duration(hours=1),
    max_consecutive_failed_dag_runs=2,
)
def tfl_roads():

    check_api = make_check_api_task(
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    @task(templates_dict={
        'start_date': '{{ data_interval_start.isoformat() }}',
        'end_date': '{{ data_interval_end.isoformat() }}',
    })
    def get_tfl_road_disruptions(templates_dict):
        endpoint = '/Road/all/Street/Disruption'
        params = {
            'startDate': templates_dict['start_date'],
            'endDate': templates_dict['end_date'],
        }
        return get_api_data(endpoint, params)

    check_api() >> get_tfl_road_disruptions()


tfl_roads()
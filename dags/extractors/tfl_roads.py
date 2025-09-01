from airflow.decorators import dag, task

from pendulum import duration, datetime

from include.tasks import make_check_api_task, make_get_data_task


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

    check_api = make_check_api_sensor(
        poke_interval=30,
        timeout=300,
        mode="reschedule",
    )

    get_tfl_road_disruptions = make_get_data_task(
        task_id='get_tfl_road_disruptions',
        endpoint='/Road/all/Street/Disruption',
        templated_params={
            'startDate': '{{ data_interval_start.isoformat() }}',
            'endDate': '{{ data_interval_end.isoformat() }}',
        }
    )

    check_api() >> get_tfl_road_disruptions()


tfl_roads()
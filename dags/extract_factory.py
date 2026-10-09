from airflow.decorators import dag

from pendulum import duration

from include.callbacks import notify_teams
from include.dag_config import ExtractDagConfig
from include.datasets import DATASETS
from include.tasks.extract_tasks import make_check_api_sensor, make_ingest_data_task


def make_extract_dag(config: ExtractDagConfig):
    @dag(
        dag_id=config.dag_id,
        start_date=config.start_date,
        schedule=config.schedule,
        catchup=False,
        description=f"This DAG extracts {config.dag_id} data",
        tags=["tfl", "extract", config.tag],
        default_args={
            "retries": config.retries,
            "on_failure_callback": notify_teams,
        },
        dagrun_timeout=config.dagrun_timeout,
        max_consecutive_failed_dag_runs=2,
    )
    def extract():

        check_api = make_check_api_sensor(
            poke_interval=config.sensor_poke_interval,
            timeout=config.sensor_timeout,
            mode=config.sensor_mode,
        )

        ingest_data = make_ingest_data_task(
            endpoint=config.endpoint,
            templated_params=config.templated_params,
            dataset=config.dataset,
        )

        check_api() >> ingest_data()

    return extract()


configs = [
    ExtractDagConfig(
        dag_id='tfl_bikes',
        tag='bikes',
        endpoint='/Place/Type/BikePoint',
        dataset=DATASETS['bike_points'].raw,
    ),

    ExtractDagConfig(
        dag_id='tfl_chargers',
        tag='chargers',
        endpoint='/Place/Type/ChargeConnector',
        dataset=DATASETS['chargers'].raw,
    ),

    ExtractDagConfig(
        dag_id='tfl_roads',
        tag='roads',
        endpoint='/Road/all/Street/Disruption',
        templated_params={
            'startDate': '{{ data_interval_start.isoformat() }}',
            'endDate': '{{ data_interval_end.isoformat() }}',
        },
        dataset=DATASETS['roads'].raw,
        schedule='@daily',
        dagrun_timeout=duration(hours=1),
        sensor_poke_interval=30,
        sensor_timeout=300,
        sensor_mode='reschedule',
    ),
]


for cfg in configs:
    globals()[cfg.dag_id] = make_extract_dag(cfg)
from airflow.decorators import dag

from pendulum import duration

from include.callbacks import notify_teams
from include.dag_config import ExtractDagConfig
from include.datasets import DATASET_E_CHARGERS, DATASET_E_ROADS, DATASET_E_BIKES
from include.tasks.common_tasks import make_emit_dataset_task
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

        check_api = config.custom_api_sensor() if config.custom_api_sensor else make_check_api_sensor()

        ingest_data = make_ingest_data_task(
            endpoint=config.endpoint,
            templated_params=config.templated_params,
            dir_name=config.dataset.uri,
        )

        emit_data = make_emit_dataset_task(
            dataset=config.dataset,
        )

        check_api() >> emit_data(path=ingest_data())

    return extract()


configs = [
    ExtractDagConfig(
        dag_id='tfl_bikes',
        tag='bikes',
        endpoint='/Place/Type/BikePoint',
        dataset=DATASET_E_BIKES,
    ),

    ExtractDagConfig(
        dag_id='tfl_chargers',
        tag='chargers',
        endpoint='/Place/Type/ChargeConnector',
        dataset=DATASET_E_CHARGERS,
    ),

    ExtractDagConfig(
        dag_id='tfl_roads',
        tag='roads',
        endpoint='/Road/all/Street/Disruption',
        templated_params={
            'startDate': '{{ data_interval_start.isoformat() }}',
            'endDate': '{{ data_interval_end.isoformat() }}',
        },
        dataset=DATASET_E_ROADS,
        schedule='@daily',
        dagrun_timeout=duration(hours=1),
        custom_api_sensor=lambda: make_check_api_sensor(
            poke_interval=30,
            timeout=300,
            mode="reschedule",
        )
    ),
]


for cfg in configs:
    globals()[cfg.dag_id] = make_extract_dag(cfg)
from airflow.sdk import CronDataIntervalTimetable, PokeReturnValue, dag, task
from pendulum import duration

from include.callbacks import notify_teams
from include.dag_config import ExtractDagConfig
from include.datasets import DATASETS, EXTRA_VAL_KEYS

# manual runs have no logical_date in Airflow 3, so ds/ts_nodash come from run_after
RAW_PATH = '{{ dag_run.run_after | ds }}/{{ dag_run.run_after | ts_nodash }}.json'


@task.sensor
def check_api(endpoint: str, query: dict) -> PokeReturnValue:
    from include.helpers.api_client import is_api_available

    return PokeReturnValue(is_done=is_api_available(endpoint, query))


@task
def ingest_data(
    source: str,
    endpoint: str,
    path: str,
    query: dict,
    outlet_events=None
) -> None:
    from include.helpers.api_client import get_api_data_stream
    from include.helpers.storage import store_stream_in_s3

    with get_api_data_stream(endpoint, query) as api_response:
        if 'gzip' in api_response.headers.get('Content-Encoding', '').lower():
            path += '.gz'

        store_stream_in_s3(api_response.raw, path)

    outlet_events[DATASETS[source].raw].extra = {EXTRA_VAL_KEYS['raw']: path}


def make_extract_dag(config: ExtractDagConfig):
    raw = DATASETS[config.source].raw

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
        query = config.templated_params or {}

        api_ready = check_api.override(
            poke_interval=config.sensor_poke_interval,
            timeout=config.sensor_timeout,
            mode=config.sensor_mode,
        )(config.endpoint, query)

        api_ready >> ingest_data.override(outlets=[raw])(
            source=config.source,
            endpoint=config.endpoint,
            path=f'{raw.uri}{RAW_PATH}',
            query=query,
        )

    return extract()


configs = [
    ExtractDagConfig(
        dag_id='tfl_bikes',
        tag='bikes',
        endpoint='/Place/Type/BikePoint',
        source='bike_points',
    ),

    ExtractDagConfig(
        dag_id='tfl_chargers',
        tag='chargers',
        endpoint='/Place/Type/ChargeConnector',
        source='chargers',
    ),

    ExtractDagConfig(
        dag_id='tfl_roads',
        tag='roads',
        endpoint='/Road/all/Street/Disruption',
        # manual runs have no data interval in Airflow 3, so they fall back to the day before run_after
        templated_params={
            'startDate': '{{ (data_interval_start | default(dag_run.run_after - macros.timedelta(days=1), true)).isoformat() }}',
            'endDate': '{{ (data_interval_end | default(dag_run.run_after, true)).isoformat() }}',
        },
        source='roads',
        schedule=CronDataIntervalTimetable('@daily', timezone='UTC'),
        dagrun_timeout=duration(hours=1),
        sensor_poke_interval=30,
        sensor_timeout=300,
        sensor_mode='reschedule',
    ),
]


for cfg in configs:
    globals()[cfg.dag_id] = make_extract_dag(cfg)

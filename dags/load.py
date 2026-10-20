from airflow.sdk import dag, task

from include.callbacks import notify_teams
from include.dag_config import START_DATE, DEFAULT_DAGRUN_TIMEOUT, DEFAULT_RETRIES
from include.datasets import DATASETS, EXTRA_VAL_KEYS


@task
def get_raw_paths(
    source: str,
    triggering_asset_events=None
) -> list[str]:
    from include.helpers.dataset_utils import get_event_extras

    return get_event_extras(
        triggering_asset_events
    ).get(
        DATASETS[source].raw.uri,
        []
    )


@task
def validate_and_load(
    source: str,
    path: str,
    outlet_events=None
) -> None:
    import gzip

    from include.helpers.dataset_utils import get_batch_id_from_path
    from include.helpers.postgres import stream_to_pg_with_dlq
    from include.helpers.storage import get_s3_obj

    batch_id = get_batch_id_from_path(path)
    body = get_s3_obj(path)['Body']

    with gzip.GzipFile(fileobj=body) if path.endswith('.gz') else body as raw_stream:
        stream_to_pg_with_dlq(raw_stream, batch_id, source)

    outlet_events[DATASETS[source].bronze].extra = {EXTRA_VAL_KEYS['bronze']: batch_id}


@dag(
    dag_id='loader',
    start_date=START_DATE,
    schedule=(
            DATASETS['bike_points'].raw
            | DATASETS['chargers'].raw
            | DATASETS['roads'].raw
    ),
    catchup=False,
    description='This DAG loads tfl data',
    tags=['tfl', 'load'],
    default_args={
        'retries': DEFAULT_RETRIES,
        'on_failure_callback': notify_teams,
    },
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
)
def load():
    for source, datasets in DATASETS.items():
        raw_paths = get_raw_paths.override(
            task_id=f'get_raw_paths__{source}'
        )(source)

        validate_and_load.override(
            task_id=f'validate_and_load__{source}',
            outlets=[datasets.bronze],
        ).partial(source=source).expand(path=raw_paths)


load()

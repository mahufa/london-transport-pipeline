from airflow.sdk import dag, task, Param
from pendulum import duration
from cosmos import DbtTaskGroup, ProfileConfig, ExecutionConfig, ProjectConfig
from cosmos.operators.local import DbtSourceLocalOperator
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

from include.callbacks import notify_teams
from include.connections import POSTGRES_CONN_ID
from include.dag_config import START_DATE
from include.datasets import DATASETS
from include.helpers.metabase import sync_schema
from include.paths import DBT_EXECUTABLE, DBT_PROJECT_DIR

profile_config = ProfileConfig(
    profile_name='london_transport_pipeline',
    target_name='airflow',
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={'schema': 'public'},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE,
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    install_dbt_deps=False,
    dbt_vars={
        'incremental_lookback': '{{ params.incremental_lookback }}',
    },
)

@dag(
    dag_id='transformer',
    start_date=START_DATE,
    schedule=(
            DATASETS['bike_points'].bronze
            | DATASETS['chargers'].bronze
            | DATASETS['roads'].bronze
    ),
    catchup=False,
    max_active_runs=1,
    description='This DAG transforms tfl data',
    tags=['tfl', 'transform'],
    default_args={
        'retries': 0,
        'on_failure_callback': notify_teams,
    },
    dagrun_timeout=duration(minutes=30),
    params={
        'incremental_lookback': Param('2 hours', type='string'),
    },
)
def transform():
    DbtSourceLocalOperator(
        task_id='check_source_freshness',
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        dbt_executable_path=DBT_EXECUTABLE,
        install_deps=False,
    )

    transform_data = DbtTaskGroup(
        group_id='transform_data',
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        on_warning_callback=notify_teams,
    )

    @task
    def sync_metabase_schema():
        sync_schema()

    transform_data >> sync_metabase_schema()


transform()

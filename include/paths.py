import os
from pathlib import Path

INCLUDE_DIR = Path(__file__).parent
SQL_DIR = INCLUDE_DIR / 'sql'
DBT_PROJECT_DIR = INCLUDE_DIR.parent / 'dbt'
DBT_EXECUTABLE = Path(os.environ.get(
    'DBT_VENV',
    '/opt/airflow/dbt_venv')
) / 'bin' / 'dbt'

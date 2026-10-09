from dataclasses import dataclass
from typing import Optional
from airflow.datasets import Dataset
from pendulum import datetime, Duration, DateTime, UTC, duration


START_DATE = datetime(2025, 6, 1).astimezone(UTC)
DEFAULT_RETRIES = 2
DEFAULT_DAGRUN_TIMEOUT = duration(minutes=10)
MAX_REJECT_RATIO = 0.1

@dataclass
class ExtractDagConfig:
    dag_id: str
    tag: str
    endpoint: str
    dataset: Dataset
    templated_params: Optional[dict] = None
    schedule: str = "*/30 * * * *"
    dagrun_timeout: Duration = DEFAULT_DAGRUN_TIMEOUT
    sensor_poke_interval: int = 10
    sensor_timeout: int = 100
    sensor_mode: str = 'poke'
    retries: int = DEFAULT_RETRIES
    start_date: DateTime = START_DATE

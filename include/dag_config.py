from dataclasses import dataclass
from typing import Callable, Optional
from airflow.datasets import Dataset
from pendulum import datetime, Duration, DateTime, UTC, duration


START_DATE = datetime(2025, 6, 1).astimezone(UTC)

@dataclass
class ExtractDagConfig:
    dag_id: str
    tag: str
    endpoint: str
    templated_params: Optional[dict] = None
    dataset: Optional[Dataset] = None
    schedule: str = "*/30 * * * *"
    dagrun_timeout: Duration = duration(minutes=10)
    custom_api_sensor: Optional[Callable] = None
    retries: int = 2
    start_date: DateTime = START_DATE
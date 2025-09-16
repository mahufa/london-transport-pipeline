from dataclasses import dataclass

from airflow.datasets import Dataset


@dataclass(frozen=True)
class LayerDatasets:
    raw: Dataset
    staging: Dataset


DATASETS: dict[str, LayerDatasets] = {
    "bike_points": LayerDatasets(
        raw=Dataset("bike_points/raw/"),
        staging=Dataset("bike_points/staging/"),
    ),
    "chargers": LayerDatasets(
        raw=Dataset("chargers/raw/"),
        staging=Dataset("chargers/staging/"),
    ),
    "roads": LayerDatasets(
        raw=Dataset("roads/raw/"),
        staging=Dataset("roads/staging/"),
    ),
}

EXTRACT_DATASETS = [ds.raw for ds in DATASETS.values()]
TRANSFORM_DATASETS = [ds.staging for ds in DATASETS.values()]


PATH_KEY = 'file_path'

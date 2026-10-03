from dataclasses import dataclass

from airflow.datasets import Dataset


@dataclass(frozen=True)
class LayerDatasets:
    raw: Dataset
    bronze: Dataset


DATASETS: dict[str, LayerDatasets] = {
    "bike_points": LayerDatasets(
        raw=Dataset("bike_points/raw/"),
        bronze=Dataset("bike_points/bronze/"),
    ),
    "chargers": LayerDatasets(
        raw=Dataset("chargers/raw/"),
        bronze=Dataset("chargers/bronze/"),
    ),
    "roads": LayerDatasets(
        raw=Dataset("roads/raw/"),
        bronze=Dataset("roads/bronze/"),
    ),
}

EXTRACT_DATASETS = [ds.raw for ds in DATASETS.values()]
TRANSFORM_DATASETS = [ds.bronze for ds in DATASETS.values()]


PATH_KEY = 'file_path'

RECORD_KEY_FIELDS = {
    'bike_points': ('id',),
    'chargers': ('id',),
    'roads': ('disruptionId', ('distruptedStreetId', 'disruptedStreetId')),
}

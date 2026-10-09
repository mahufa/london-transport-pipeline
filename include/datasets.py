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

EXTRA_VAL_KEYS = {
    'raw': 'path',
    'bronze': 'batch_id',
}

RECORD_KEY_FIELDS = {
    'bike_points': ('id',),
    'chargers': ('id',),
    'roads': ('disruptionId', ('distruptedStreetId', 'disruptedStreetId')),
}

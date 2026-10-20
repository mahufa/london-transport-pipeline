from dataclasses import dataclass

from airflow.sdk import Asset


@dataclass(frozen=True)
class LayerDatasets:
    raw: Asset
    bronze: Asset


DATASETS: dict[str, LayerDatasets] = {
    "bike_points": LayerDatasets(
        raw=Asset("bike_points/raw/"),
        bronze=Asset("bike_points/bronze/"),
    ),
    "chargers": LayerDatasets(
        raw=Asset("chargers/raw/"),
        bronze=Asset("chargers/bronze/"),
    ),
    "roads": LayerDatasets(
        raw=Asset("roads/raw/"),
        bronze=Asset("roads/bronze/"),
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

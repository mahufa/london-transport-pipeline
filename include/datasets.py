from airflow.datasets import Dataset

DATASET_BIKES = Dataset('bike_points/raw/')
DATASET_CHARGERS = Dataset('chargers/raw/')
DATASET_ROADS = Dataset('roads/raw/')

ALL_DATASETS = [
    DATASET_BIKES,
    DATASET_CHARGERS,
    DATASET_ROADS,
]


PATH_KEY = 'file_path'

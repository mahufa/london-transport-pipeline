from airflow.datasets import Dataset

DATASET_E_BIKES = Dataset('bike_points/raw/')
DATASET_E_CHARGERS = Dataset('chargers/raw/')
DATASET_E_ROADS = Dataset('roads/raw/')

EXTRACT_DATASETS = [
    DATASET_E_BIKES,
    DATASET_E_CHARGERS,
    DATASET_E_ROADS,
]

DATASET_T_BIKES = Dataset('bike_points/staging/')
DATASET_T_CHARGERS = Dataset('chargers/staging/')
DATASET_T_ROADS = Dataset('roads/staging/')

TRANSFORM_DATASETS = [
    DATASET_T_BIKES,
    DATASET_T_CHARGERS,
    DATASET_T_ROADS,
]


PATH_KEY = 'file_path'

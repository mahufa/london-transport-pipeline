from include.cleaners.bike_points.bike_points import clean_bike_points
from include.cleaners.chargers.chargers import clean_chargers
from include.cleaners.roads.roads import clean_roads


cleaners_registry = {
    'bike_points/raw/': clean_bike_points,
    'chargers/raw/': clean_chargers,
    'roads/raw/': clean_roads,
}


def clean_dataset(
    dataset_uri: str,
    raw_data: str,
    batch_id: str,
) -> str:
    """Clean raw data json string, returns csv formated string"""
    cleaner_func = cleaners_registry.get(dataset_uri)
    if not cleaner_func:
        raise ValueError(f'No cleaner found for dataset: {dataset_uri}')

    return cleaner_func(raw_data, batch_id).to_csv(index=False)

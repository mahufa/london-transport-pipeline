import pandas as pd

from include.cleaners.common import normalize_columns_names, add_batch_id


def clean_roads(
    raw_data: str,
    batch_id: str
) -> pd.DataFrame:
    return (
        _read_necessary_columns(raw_data)
        .pipe(_adjust_id_columns)
        .pipe(_parse_dt_columns)
        .pipe(normalize_columns_names)
        .pipe(add_batch_id, batch_id)
    )


def _read_necessary_columns(raw_data: str) -> pd.DataFrame:
    return pd.read_json(
                raw_data.strip()
            ).drop(
                labels=[
                    '$type',
                    'lineString',
                    'comments',
                    'levelOfInterest',
                    'location',
                    'recurringSchedules',
                ],
                axis='columns'
            )

def _adjust_id_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['disruptionId'] = df['disruptionId'].str.replace('TIMS-', '').astype('int64')
    df.rename(
        columns={
            'distruptedStreetId': 'disrupted_road_id', # original spelling from TfL
            'disruptedStreetId': 'disrupted_road_id', # if they decide to correct
        },
        inplace=True
    )
    return df

def _parse_dt_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['startDateTime'] = pd.to_datetime(df['startDateTime'], format='ISO8601')
    df['endDateTime'] = pd.to_datetime(df['endDateTime'], format='ISO8601')
    return df

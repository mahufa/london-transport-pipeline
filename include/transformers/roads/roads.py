from pandas import DataFrame, read_json, to_datetime

from include.transformers.common import normalize_column_names


def clean_roads(raw_data: str) -> DataFrame:
    return (_read_necessary_columns(raw_data)
            .pipe(_adjust_id_columns)
            .pipe(_parse_dt_columns)
            .pipe(normalize_column_names)
            )


def _read_necessary_columns(raw_data: str) -> DataFrame:
    return read_json(
                raw_data.strip()
            ).drop(
                labels=['$type', 'lineString', 'comments', 'levelOfInterest', 'recurringSchedules'],
                axis='columns'
            )

def _adjust_id_columns(df: DataFrame) -> DataFrame:
    df['disruptionId'] = df['disruptionId'].str.replace('TIMS-', '').astype('int64')
    df.rename(
        columns={
            'distruptedStreetId': 'disrupted_road_id', # original spelling from TfL
            'disruptedStreetId': 'disrupted_road_id', # if they decide to correct
        },
        inplace=True
    )
    return df

def _parse_dt_columns(df: DataFrame) -> DataFrame:
    df['startDateTime'] = to_datetime(df['startDateTime'], format='ISO8601')
    df['endDateTime'] = to_datetime(df['endDateTime'], format='ISO8601')
    return df

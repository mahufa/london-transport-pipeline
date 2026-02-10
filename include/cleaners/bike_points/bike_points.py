import pandas as pd

from include.cleaners.common import read_necessary_columns, reshape, normalize_columns_names, \
    add_batch_id


def clean_bike_points(
    raw_data: str,
    batch_id: str
) -> pd.DataFrame:
    return (
        read_necessary_columns(raw_data)
            .pipe(reshape)
            .pipe(_adjust_id_column)
            .pipe(_parse_columns)
            .pipe(normalize_columns_names)
            .pipe(add_batch_id, batch_id)
    )


def _adjust_id_column(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        id = df['id'].str.replace('BikePoints_', '').astype('int16')
    ).rename(columns={'id':'bike_point_id'})

def _parse_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_parse = ['NbBikes', 'NbStandardBikes', 'NbEBikes', 'NbEmptyDocks', 'NbDocks']
    df[cols_to_parse] = df[cols_to_parse].astype('int16')

    return df

import pandas as pd

from include.transformers.common import read_necessary_columns, reshape_additional_props, normalize_column_names


def clean_bike_points(raw_data: str) -> pd.DataFrame:
    return (
        read_necessary_columns(raw_data)
            .pipe(
                reshape_additional_props,
                prop_to_extract_date_from='NbEmptyDocks',
                props_to_drop=[
                    'TerminalName',
                    'Installed',
                    'Locked',
                    'InstallDate',
                    'RemovalDate',
                    'Temporary',
                ],
            )
            .pipe(_adjust_id_column)
            .pipe(_parse_columns)
            .pipe(normalize_column_names)
    )


def _adjust_id_column(df: pd.DataFrame) -> pd.DataFrame:
    df['id'] = df['id'].str.replace('BikePoints_', '').astype('int16')
    df.rename(columns={'id':'bike_point_id'}, inplace=True)

    return df


def _parse_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_parse = ['NbBikes', 'NbStandardBikes', 'NbEBikes', 'NbEmptyDocks', 'NbDocks']
    df[cols_to_parse] = df[cols_to_parse].astype('int16')

    return df

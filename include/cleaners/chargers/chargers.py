import pandas as pd

from include.cleaners.common import read_necessary_columns, reshape_additional_props, normalize_columns_names, add_batch_id


def clean_chargers(
    raw_data: str,
    batch_id: str
) -> pd.DataFrame:
    return (
        read_necessary_columns(raw_data)
        .pipe(
            reshape_additional_props,
            prop_to_extract_date_from='Status',
            props_to_drop=[
                'ConnectorDescription',
                'LastUpdated',
            ],
        )
        .pipe(_adjust_id_column)
        .pipe(_adjust_power_prop_column)
        .pipe(normalize_columns_names)
        .pipe(add_batch_id, batch_id)
    )


def _adjust_id_column(df: pd.DataFrame) -> pd.DataFrame:
    df['id'] = df['id'].str.split('-', n=1).str[1]
    df.rename(columns={'id':'connector_id'}, inplace=True)
    return df


def _adjust_power_prop_column(df: pd.DataFrame) -> pd.DataFrame:
    df['Power'] = df['Power'].str.replace('kW', '').astype('int16')
    df.rename(columns={'Power':'power_kw'}, inplace=True)
    return df

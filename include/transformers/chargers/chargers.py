from pandas import DataFrame

from include.transformers.common import read_necessary_columns, reshape_additional_props, normalize_column_names


def clean_chargers(raw_data: str) -> DataFrame:
    return (
        read_necessary_columns(raw_data)
        .pipe(
            reshape_additional_props,
            prop_to_extract_date_from='Status',
            props_to_drop=[
                'ConnectorDescription',
                'Volts',
                'Amps',
                'Phase',
                'LastUpdated',
            ],
        )
        .pipe(_adjust_id_column)
        .pipe(_adjust_power_prop_column)
        .pipe(normalize_column_names)
    )


def _adjust_id_column(df: DataFrame) -> DataFrame:
    df['id'] = df['id'].str.split('-', n=1).str[1]
    df.rename(columns={'id':'connector_id'}, inplace=True)
    return df


def _adjust_power_prop_column(df: DataFrame) -> DataFrame:
    df['Power'] = df['Power'].str.replace('kW', '').astype('int16')
    df.rename(columns={'Power':'power_kw'}, inplace=True)
    return df

import json
from re import sub

import pandas as pd


def add_batch_id(
    df: pd.DataFrame,
    batch_id: str
) -> pd.DataFrame:
    df['batch_id'] = batch_id
    return df


def read_necessary_columns(raw_data: str) -> pd.DataFrame:
    data = json.loads(raw_data)
    return pd.json_normalize(
                data,
                record_path='additionalProperties',
                meta=['id', 'commonName', 'lat', 'lon']
            ).drop(axis='columns', labels=['$type', 'category', 'sourceSystemKey'])


def reshape(df: pd.DataFrame) -> pd.DataFrame:
    last_update = pd.to_datetime(
        df.groupby('id').modified.max(),
        format='ISO8601'
    )
    pivoted = df.pivot_table(
        columns='key',
        values='value',
        index=['id', 'commonName', 'lat', 'lon'],
        aggfunc='first'
    ).reset_index()

    return pivoted.merge(
        last_update,
        on='id',
        how='left'
    ).rename(
        columns={'modified': 'updated_at'}
    )


def normalize_columns_names(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Should be used in last .pipe()"""

    df.columns = [_normalize_column_name(c) for c in df.columns]
    return df


def _normalize_column_name(name: str) -> str:
    normalized_name = sub(r"([A-Z]+)([A-Z][a-z])", r'\1_\2', name)
    normalized_name = sub(r"([a-z\d])([A-Z])", r'\1_\2', normalized_name)

    return normalized_name.lower()
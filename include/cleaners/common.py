from re import sub

import pandas as pd


def add_batch_id(
    df: pd.DataFrame,
    batch_id: str
) -> pd.DataFrame:
    df['batch_id'] = batch_id
    return df


def read_necessary_columns(raw_data: str) -> pd.DataFrame:
    return pd.read_json(
                raw_data.strip()
            ).drop(
                labels=['$type', 'url', 'placeType', 'children', 'childrenUrls'],
                axis='columns'
            )


def reshape_additional_props(
    df: pd.DataFrame,
    prop_to_extract_date_from: str,
    props_to_drop: list[str],
) -> pd.DataFrame:
    return (
        explode_additional_props(df)
          .pipe(extract_fields_from_additional_props)
          .pipe(
            pivot_additional_props,
            prop_to_extract_date_from=prop_to_extract_date_from,
          )
          .drop(props_to_drop, axis='columns')
    )


def explode_additional_props(df: pd.DataFrame) -> pd.DataFrame:
    return df.explode(
        column='additionalProperties',
        ignore_index=True,
    )


def extract_fields_from_additional_props(df: pd.DataFrame) -> pd.DataFrame:
    df_props = pd.json_normalize(df['additionalProperties'])

    return (df.drop(
                    columns='additionalProperties',
                )
                .join(df_props[['key', 'value', 'modified']])
            )


def pivot_additional_props(
        df: pd.DataFrame,
        prop_to_extract_date_from: str,
) -> pd.DataFrame:
    modified_at_col = extract_modified_dates(df, prop_to_extract_date_from)

    return (
        df.pivot_table(
                index=['id', 'commonName', 'lat', 'lon'],
                columns='key',
                values='value',
                aggfunc='first',
            )
            .reset_index()
            .join(modified_at_col, on='id')
    )


def extract_modified_dates(
        df: pd.DataFrame,
        prop_to_extraxt_date_from: str,
) -> pd.DataFrame:
    mask = df['key'] == prop_to_extraxt_date_from
    modified = df.loc[mask, ['id', 'modified']].set_index('id')
    modified['modified'] = pd.to_datetime(modified['modified'], format='ISO8601')
    modified.rename(columns={'modified':'updated_at'}, inplace=True)

    return modified


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
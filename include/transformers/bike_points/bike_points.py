from pandas import DataFrame, read_json, json_normalize


def clean_bike_points(raw_data: str) -> DataFrame:
    return (
        _read_necessary_columns(raw_data)
            .pipe(_adjust_id_column)
            .pipe(_reshape_additional_props)
            .pipe(_parse_columns)
    )


def _read_necessary_columns(raw_data: str) -> DataFrame:
    return read_json(
                raw_data.strip()
            ).drop(
                labels=['$type', 'url', 'placeType', 'children', 'childrenUrls'],
                axis='columns',
            )


def _adjust_id_column(df: DataFrame) -> DataFrame:
    df['id'] = df['id'].str.replace('BikePoints_', '').astype('int16')

    return df


def _parse_columns(df: DataFrame) -> DataFrame:
    cols_to_parse = ['NbBikes', 'NbStandardBikes', 'NbEBikes', 'NbEmptyDocks', 'NbDocks']
    df[cols_to_parse] = df[cols_to_parse].astype('int16')

    return df


def _reshape_additional_props(df: DataFrame) -> DataFrame:
    return (_explode_additional_props(df)
          .pipe(_extract_key_value_from_additional_props)
          .pipe(_pivot_additional_props)
          .pipe(_drop_unused_columns)
    )


def _explode_additional_props(df: DataFrame) -> DataFrame:
    return df.explode(
        column='additionalProperties',
        ignore_index=True,
    )


def _extract_key_value_from_additional_props(df: DataFrame) -> DataFrame:
    df_props = json_normalize(df["additionalProperties"])

    return (df.drop(columns='additionalProperties')
                .join(df_props[["key", "value"]]))


def _pivot_additional_props(df: DataFrame) -> DataFrame:
    return df.pivot_table(
            index=['id', 'commonName', 'lat', 'lon'],
            columns='key',
            values='value',
            aggfunc='first',
        ).reset_index()


def _drop_unused_columns(df: DataFrame) -> DataFrame:
    return df.drop(
        columns=[
            'TerminalName',
            'Installed',
            'Locked',
            'InstallDate',
            'RemovalDate',
            'Temporary',
        ],
    )
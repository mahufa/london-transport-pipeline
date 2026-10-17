SELECT
    connector_id,
    station_name,
    lat,
    lon,
    connector_type,
    parent_station,
    power_kw,
    status,
    updated_at,
    batch_id,
    loaded_at
FROM {{ ref('stg_chargers') }}
WHERE error IS NULL

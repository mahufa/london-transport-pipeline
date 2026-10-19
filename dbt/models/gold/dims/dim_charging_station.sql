WITH stations AS (
SELECT
    parent_station AS charging_station_id,
    station_name,
    lat,
    lon,
    row_number() OVER (
        PARTITION BY parent_station
        ORDER BY loaded_at DESC, connector_id ASC
    ) AS rn
FROM {{ ref('int_validated_chargers') }}
)

SELECT
    charging_station_id,
    station_name,
    lat,
    lon
FROM stations
WHERE rn = 1

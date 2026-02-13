BEGIN;

INSERT INTO dim_connector(
    tfl_id,
    power_kw,
    connector_type
)
SELECT DISTINCT
    connector_id,
    power_kw,
    connector_type
FROM staging_chargers
WHERE batch_id = %(batch_id)s
ON CONFLICT (tfl_id) DO UPDATE SET
    power_kw = excluded.power_kw,
    connector_type = excluded.connector_type
;


INSERT INTO dim_charging_station(
    tfl_station_id,
    name,
    lat,
    lon
)
SELECT DISTINCT
    parent_station,
    station_name,
    lat,
    lon
FROM staging_chargers
WHERE batch_id = %(batch_id)s
ON CONFLICT (tfl_station_id) DO UPDATE SET
    name = excluded.name,
    lat = excluded.lat,
    lon = excluded.lon
;


WITH chargers_with_ids AS (
    SELECT
        dcs.id dim_station_id,
        dc.id dim_connector_id,
        sc.updated_at,
        sc.status
    FROM staging_chargers sc
    JOIN dim_charging_station dcs
    ON dcs.tfl_station_id = sc.parent_station
    JOIN public.dim_connector dc
    ON dc.tfl_id = sc.connector_id
    WHERE sc.batch_id = %(batch_id)s
)
INSERT INTO fct_connector_availability_change(
      charging_station_id,
      connector_id,
      updated_at,
      status
)
SELECT
        dim_station_id,
        dim_connector_id,
        updated_at,
        status
FROM chargers_with_ids
ON CONFLICT (
    charging_station_id,
    connector_id,
    updated_at
    ) DO NOTHING;


DELETE FROM public.staging_chargers WHERE batch_id = %(batch_id)s;

COMMIT;
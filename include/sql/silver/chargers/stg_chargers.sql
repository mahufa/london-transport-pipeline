DROP VIEW IF EXISTS silver.stg_chargers;

CREATE VIEW silver.stg_chargers AS
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
    batch_id
FROM silver.checked_chargers
WHERE error IS NULL;

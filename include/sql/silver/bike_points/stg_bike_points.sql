DROP VIEW IF EXISTS silver.stg_bike_points;

CREATE VIEW silver.stg_bike_points AS
SELECT
    bike_point_id,
    common_name,
    lat,
    lon,
    nb_bikes,
    nb_docks,
    nb_e_bikes,
    nb_empty_docks,
    nb_standard_bikes,
    updated_at,
    batch_id
FROM silver.checked_bike_points
WHERE error IS NULL;

BEGIN;

INSERT INTO dim_bike_point(
    tfl_id,
    common_name,
    lat,
    lon
)
SELECT DISTINCT
    bike_point_id,
    common_name,
    lat,
    lon
FROM staging_bike_points
WHERE batch_id = %(batch_id)s
ON CONFLICT (tfl_id) DO UPDATE SET
    common_name = excluded.common_name,
    lat = excluded.lat,
    lon = excluded.lon
;


WITH bikes_with_ids AS (
    SELECT
        dbp.id dim_bp_id,
        sbp.updated_at,
        sbp.nb_standard_bikes,
        sbp.nb_e_bikes,
        sbp.nb_empty_docks,
        sbp.nb_docks
    FROM staging_bike_points sbp
    JOIN dim_bike_point dbp
    ON dbp.tfl_id = sbp.bike_point_id
    WHERE sbp.batch_id = %(batch_id)s
)
INSERT INTO fct_bikes_availability_change(
    bike_point_id,
    updated_at,
    nb_standard_bikes,
    nb_e_bikes,
    nb_empty_docks,
    nb_docks
) SELECT
      dim_bp_id,
      updated_at,
      nb_standard_bikes,
      nb_e_bikes,
      nb_empty_docks,
      nb_docks
FROM bikes_with_ids
ON CONFLICT (
    bike_point_id,
    updated_at
) DO NOTHING;


DELETE FROM staging_bike_points WHERE batch_id = %(batch_id)s;

COMMIT;
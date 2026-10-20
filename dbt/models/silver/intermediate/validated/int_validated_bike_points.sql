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
    batch_id,
    loaded_at
FROM {{ ref('stg_bike_points') }}
WHERE error IS NULL

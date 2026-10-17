{{ config(unique_key=['bike_point_id', 'updated_at']) }}
WITH changes AS (
SELECT
    bike_point_id,
    updated_at,
    nb_standard_bikes,
    nb_e_bikes,
    nb_empty_docks,
    nb_docks,
    batch_id,
    loaded_at,
    row_number() OVER (
        PARTITION BY bike_point_id, updated_at
        ORDER BY loaded_at ASC
    ) AS rn
FROM {{ ref('int_validated_bike_points') }}
{% if is_incremental() %}
WHERE loaded_at > (SELECT max(loaded_at) FROM {{ this }})
{% endif %}
)

SELECT
    bike_point_id,
    updated_at,
    nb_standard_bikes,
    nb_e_bikes,
    nb_empty_docks,
    nb_docks,
    batch_id,
    loaded_at
FROM changes
WHERE rn = 1

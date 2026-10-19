{{ config(
    incremental_strategy='append',
    indexes=[{'columns': ['bike_point_id', 'updated_at'], 'unique': True}]
) }}
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
FROM {{ ref('int_validated_bike_points') }} v
{% if is_incremental() %}
WHERE {{ incremental_since('loaded_at') }}
    AND {{ not_in_this(['bike_point_id', 'updated_at']) }}
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

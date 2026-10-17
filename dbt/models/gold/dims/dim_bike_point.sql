WITH bike_points AS (
SELECT
    bike_point_id,
    common_name,
    lat,
    lon,
    row_number() OVER (
        PARTITION BY bike_point_id
        ORDER BY loaded_at DESC
    ) AS rn
FROM {{ ref('int_validated_bike_points') }}
)

SELECT
    bike_point_id,
    common_name,
    lat,
    lon
FROM bike_points
WHERE rn = 1

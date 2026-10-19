WITH segments AS (
SELECT
    disrupted_segment_id,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    street_name,
    row_number() OVER (
        PARTITION BY disrupted_segment_id
        ORDER BY loaded_at DESC
    ) AS rn
FROM {{ ref('int_validated_roads') }}
)

SELECT
    disrupted_segment_id,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    street_name
FROM segments
WHERE rn = 1

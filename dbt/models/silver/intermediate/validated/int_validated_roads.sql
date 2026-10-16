SELECT
    street_name,
    closure,
    directions,
    disrupted_segment_id,
    disruption_id,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    severity,
    category,
    subcategory,
    start_date_time,
    end_date_time,
    batch_id,
    loaded_at
FROM {{ ref('stg_roads') }}
WHERE error IS NULL

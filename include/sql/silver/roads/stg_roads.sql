DROP VIEW IF EXISTS silver.stg_roads;

CREATE VIEW silver.stg_roads AS
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
    batch_id
FROM silver.checked_roads
WHERE error IS NULL;

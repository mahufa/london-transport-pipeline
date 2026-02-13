BEGIN;

INSERT INTO dim_disruption(
    tfl_id,
    category,
    subcategory,
    severity
)
SELECT DISTINCT
    disruption_id,
    category,
    subcategory,
    severity
FROM staging_roads
WHERE batch_id = %(batch_id)s
ON CONFLICT (tfl_id) DO UPDATE SET
    category = excluded.category,
    subcategory = excluded.subcategory,
    severity = excluded.severity
;


INSERT INTO dim_street_segment(
    tfl_id,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    street_name
)
SELECT DISTINCT
    disrupted_segment_id,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    street_name
FROM staging_roads
WHERE batch_id = %(batch_id)s
ON CONFLICT (tfl_id) DO UPDATE SET
    start_lat = excluded.start_lat,
    start_lon = excluded.start_lon,
    end_lat = excluded.end_lat,
    end_lon = excluded.end_lon,
    street_name = excluded.street_name
;


INSERT INTO dim_closure_type(
    closure,
    directions
)
SELECT DISTINCT
    closure,
    directions
FROM staging_roads
WHERE batch_id = %(batch_id)s
ON CONFLICT (
    closure,
    directions
)DO NOTHING;


WITH roads_with_ids AS (
    SELECT
        dct.id dim_closure_id,
        dd.id dim_disruption_id,
        ds.id dim_segment_id,
        sr.start_date_time,
        sr.end_date_time
    FROM staging_roads sr
    JOIN dim_closure_type dct
    ON dct.closure = sr.closure
        AND dct.directions = sr.directions
    JOIN dim_disruption dd
    ON dd.tfl_id = sr.disruption_id
    JOIN dim_street_segment ds
    ON ds.tfl_id = sr.disrupted_segment_id
    WHERE sr.batch_id = %(batch_id)s
)
INSERT INTO fct_disrupted_segment(
    closure_type_id,
    disruption_id,
    segment_id,
    start_date_time,
    end_date_time
)
SELECT
    dim_closure_id,
    dim_disruption_id,
    dim_segment_id,
    start_date_time,
    end_date_time
FROM roads_with_ids
ON CONFLICT(
    closure_type_id,
    disruption_id,
    segment_id
) DO UPDATE SET
    start_date_time = excluded.start_date_time,
    end_date_time = excluded.end_date_time
;


DELETE FROM public.staging_roads WHERE batch_id = %(batch_id)s;

COMMIT;
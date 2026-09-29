DROP VIEW IF EXISTS silver.checked_roads CASCADE;

CREATE VIEW silver.checked_roads AS
WITH extracted AS (
SELECT
    r.batch_id,
    r.record_key,
    r.payload->>'streetName' AS street_name_raw,
    r.payload->>'closure' AS closure_raw,
    r.payload->>'directions' AS directions_raw,
    COALESCE(
        r.payload->>'distruptedStreetId', -- TfL typo
        r.payload->>'disruptedStreetId'
    ) AS segment_id_raw,
    replace(
        r.payload->>'disruptionId',
        'TIMS-',
        ''
    ) AS disruption_id_raw,
    r.payload->>'startLat' AS start_lat_raw,
    r.payload->>'startLon' AS start_lon_raw,
    r.payload->>'endLat' AS end_lat_raw,
    r.payload->>'endLon' AS end_lon_raw,
    r.payload->>'severity' AS severity_raw,
    r.payload->>'category' AS category_raw,
    r.payload->>'subCategory' AS subcategory_raw,
    r.payload->>'startDateTime' AS start_raw,
    r.payload->>'endDateTime' AS end_raw
FROM bronze.raw_tfl r
WHERE r.source = 'roads'
), validated AS (
SELECT
    e.*,
    NULLIF(CONCAT_WS('; ',
        CASE
            WHEN NOT silver.is_valid(street_name_raw, 'varchar(50)')
            THEN 'invalid streetName'
        END,
        CASE
            WHEN NOT silver.is_valid(closure_raw, 'varchar(20)')
            THEN 'invalid closure'
        END,
        CASE
            WHEN NOT silver.is_valid(directions_raw, 'varchar(30)')
            THEN 'invalid directions'
        END,
        CASE
            WHEN NOT silver.is_valid(segment_id_raw, 'char(17)')
            THEN 'invalid disruptedStreetId'
        END,
        CASE
            WHEN NOT silver.is_valid(disruption_id_raw, 'int')
            THEN 'invalid disruptionId'
        END,
        CASE
            WHEN NOT silver.is_valid(start_lat_raw, 'decimal(8,5)')
                OR NOT silver.is_valid(start_lon_raw, 'decimal(8,5)')
                OR NOT silver.is_valid(end_lat_raw, 'decimal(8,5)')
                OR NOT silver.is_valid(end_lon_raw, 'decimal(8,5)')
            THEN 'invalid coordinates'
        END,
        CASE
            WHEN NOT silver.is_valid(severity_raw, 'varchar(20)')
            THEN 'invalid severity'
        END,
        CASE
            WHEN NOT silver.is_valid(category_raw, 'varchar(20)')
            THEN 'invalid category'
        END,
        CASE
            WHEN NOT silver.is_valid(subcategory_raw, 'varchar(30)')
            THEN 'invalid subCategory'
        END,
        CASE
            WHEN NOT silver.is_valid(start_raw, 'timestamptz')
            THEN 'invalid startDateTime'
        END,
        CASE
            WHEN NOT silver.is_valid(end_raw, 'timestamptz')
            THEN 'invalid endDateTime'
        END
    ), '') AS error
FROM extracted e
) SELECT
    batch_id,
    record_key,
    error,
    CASE
        WHEN error IS NULL
        THEN street_name_raw::varchar(50)
    END AS street_name,
    CASE
        WHEN error IS NULL
        THEN closure_raw::varchar(20)
    END AS closure,
    CASE
        WHEN error IS NULL
        THEN directions_raw::varchar(30)
    END AS directions,
    CASE
        WHEN error IS NULL
        THEN segment_id_raw::char(17)
    END AS disrupted_segment_id,
    CASE
        WHEN error IS NULL
        THEN disruption_id_raw::int
    END AS disruption_id,
    CASE
        WHEN error IS NULL
        THEN start_lat_raw::decimal(8,5)
    END AS start_lat,
    CASE
        WHEN error IS NULL
        THEN start_lon_raw::decimal(8,5)
    END AS start_lon,
    CASE
        WHEN error IS NULL
        THEN end_lat_raw::decimal(8,5)
    END AS end_lat,
    CASE
        WHEN error IS NULL
        THEN end_lon_raw::decimal(8,5)
    END AS end_lon,
    CASE
        WHEN error IS NULL
        THEN severity_raw::varchar(20)
    END AS severity,
    CASE
        WHEN error IS NULL
        THEN category_raw::varchar(20)
    END AS category,
    CASE
        WHEN error IS NULL
        THEN subcategory_raw::varchar(30)
    END AS subcategory,
    CASE
        WHEN error IS NULL
        THEN start_raw::timestamptz
    END AS start_date_time,
    CASE
        WHEN error IS NULL
        THEN end_raw::timestamptz
    END AS end_date_time
FROM validated;

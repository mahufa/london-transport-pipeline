WITH extracted AS (
SELECT
    r.batch_id,
    r.record_key,
    r.loaded_at,
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
FROM {{ source('bronze', 'raw_tfl') }} r
WHERE r.source = 'roads'
),

validated AS (
SELECT
    e.*,
    NULLIF(CONCAT_WS('; ',
        {{ error_if_invalid('street_name_raw', 'varchar(50)', 'streetName') }},
        {{ error_if_invalid('closure_raw', 'varchar(20)', 'closure') }},
        {{ error_if_invalid('directions_raw', 'varchar(30)', 'directions') }},
        {{ error_if_invalid('segment_id_raw', 'char(17)', 'disruptedStreetId') }},
        {{ error_if_invalid('disruption_id_raw', 'int', 'disruptionId') }},
        CASE
            WHEN NOT {{ is_valid('start_lat_raw', 'decimal(8,5)') }}
                OR NOT {{ is_valid('start_lon_raw', 'decimal(8,5)') }}
                OR NOT {{ is_valid('end_lat_raw', 'decimal(8,5)') }}
                OR NOT {{ is_valid('end_lon_raw', 'decimal(8,5)') }}
            THEN 'invalid coordinates'
        END,
        {{ error_if_invalid('severity_raw', 'varchar(20)', 'severity') }},
        {{ error_if_invalid('category_raw', 'varchar(20)', 'category') }},
        {{ error_if_invalid('subcategory_raw', 'varchar(30)', 'subCategory') }},
        {{ error_if_invalid('start_raw', 'timestamptz', 'startDateTime') }},
        {{ error_if_invalid('end_raw', 'timestamptz', 'endDateTime') }}
    ), '') AS error
FROM extracted e
)

SELECT
    batch_id,
    record_key,
    loaded_at,
    error,
    {{ cast_if_valid('street_name_raw', 'varchar(50)') }} AS street_name,
    {{ cast_if_valid('closure_raw', 'varchar(20)') }} AS closure,
    {{ cast_if_valid('directions_raw', 'varchar(30)') }} AS directions,
    {{ cast_if_valid('segment_id_raw', 'char(17)') }} AS disrupted_segment_id,
    {{ cast_if_valid('disruption_id_raw', 'int') }} AS disruption_id,
    {{ cast_if_valid('start_lat_raw', 'decimal(8,5)') }} AS start_lat,
    {{ cast_if_valid('start_lon_raw', 'decimal(8,5)') }} AS start_lon,
    {{ cast_if_valid('end_lat_raw', 'decimal(8,5)') }} AS end_lat,
    {{ cast_if_valid('end_lon_raw', 'decimal(8,5)') }} AS end_lon,
    {{ cast_if_valid('severity_raw', 'varchar(20)') }} AS severity,
    {{ cast_if_valid('category_raw', 'varchar(20)') }} AS category,
    {{ cast_if_valid('subcategory_raw', 'varchar(30)') }} AS subcategory,
    {{ cast_if_valid('start_raw', 'timestamptz') }} AS start_date_time,
    {{ cast_if_valid('end_raw', 'timestamptz') }} AS end_date_time
FROM validated

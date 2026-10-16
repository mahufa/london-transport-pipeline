WITH extracted AS (
SELECT
    r.batch_id,
    r.record_key,
    r.loaded_at,
    replace(
        r.payload->>'id',
        'BikePoints_',
        ''
    ) AS bike_point_id_raw,
    r.payload->>'commonName' AS common_name_raw,
    r.payload->>'lat' AS lat_raw,
    r.payload->>'lon' AS lon_raw,
    p.*
FROM {{ source('bronze', 'raw_tfl') }} r
CROSS JOIN LATERAL (
    SELECT
        {{ extract_prop('NbBikes') }} AS nb_bikes_raw,
        {{ extract_prop('NbDocks') }} AS nb_docks_raw,
        {{ extract_prop('NbEBikes') }} AS nb_e_bikes_raw,
        {{ extract_prop('NbEmptyDocks') }} AS nb_empty_docks_raw,
        {{ extract_prop('NbStandardBikes') }} AS nb_standard_bikes_raw,
        max(
            CASE
                WHEN {{ is_valid("prop->>'modified'", 'timestamptz') }}
                THEN (prop->>'modified')::timestamptz
            END
        ) AS updated_at
    FROM jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(r.payload->'additionalProperties') = 'array'
            THEN r.payload->'additionalProperties'
            ELSE '[]'::jsonb
        END
    ) prop
) p
WHERE r.source = 'bike_points'
),

validated AS (
SELECT
    e.*,
    NULLIF(CONCAT_WS('; ',
        {{ error_if_invalid('bike_point_id_raw', 'int', 'id') }},
        {{ error_if_invalid('common_name_raw', 'varchar(100)', 'commonName') }},
        CASE
            WHEN NOT {{ is_valid('lat_raw', 'decimal(8,5)') }}
                OR NOT {{ is_valid('lon_raw', 'decimal(8,5)') }}
            THEN 'invalid coordinates'
        END,
        {{ error_if_invalid('nb_bikes_raw', 'int', 'NbBikes') }},
        {{ error_if_invalid('nb_docks_raw', 'int', 'NbDocks') }},
        {{ error_if_invalid('nb_e_bikes_raw', 'int', 'NbEBikes') }},
        {{ error_if_invalid('nb_empty_docks_raw', 'int', 'NbEmptyDocks') }},
        {{ error_if_invalid('nb_standard_bikes_raw', 'int', 'NbStandardBikes') }},
        {{ error_if_null('updated_at', 'modified') }}
    ), '') AS error
FROM extracted e
)

SELECT
    batch_id,
    record_key,
    error,
    loaded_at,
    {{ cast_if_valid('bike_point_id_raw', 'int') }} AS bike_point_id,
    {{ cast_if_valid('common_name_raw', 'varchar(100)') }} AS common_name,
    {{ cast_if_valid('lat_raw', 'decimal(8,5)') }} AS lat,
    {{ cast_if_valid('lon_raw', 'decimal(8,5)') }} AS lon,
    {{ cast_if_valid('nb_bikes_raw', 'int') }} AS nb_bikes,
    {{ cast_if_valid('nb_docks_raw', 'int') }} AS nb_docks,
    {{ cast_if_valid('nb_e_bikes_raw', 'int') }} AS nb_e_bikes,
    {{ cast_if_valid('nb_empty_docks_raw', 'int') }} AS nb_empty_docks,
    {{ cast_if_valid('nb_standard_bikes_raw', 'int') }} AS nb_standard_bikes,
    {{ cast_if_valid('updated_at', 'timestamptz') }} AS updated_at
FROM validated

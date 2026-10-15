WITH extracted AS (
SELECT
    r.batch_id,
    r.record_key,
    r.loaded_at,
    CASE
        WHEN strpos(r.payload->>'id', '-') > 0
        THEN substr(r.payload->>'id', strpos(r.payload->>'id', '-') + 1)
    END AS connector_id_raw,
    COALESCE(
        substring(r.payload->>'commonName' FROM '^(.*) Connector '),
        r.payload->>'commonName'
    ) AS station_name_raw,
    r.payload->>'lat' AS lat_raw,
    r.payload->>'lon' AS lon_raw,
    p.*
FROM {{ source('bronze', 'raw_tfl') }} r
CROSS JOIN LATERAL (
    SELECT
        {{ extract_prop('ConnectorType') }} AS connector_type_raw,
        {{ extract_prop('ParentStation') }} AS parent_station_raw,
        replace({{ extract_prop('Power') }}, 'kW', '') AS power_kw_raw,
        {{ extract_prop('Status') }} AS status_raw,
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
WHERE r.source = 'chargers'
),

validated AS (
SELECT
    e.*,
    NULLIF(CONCAT_WS('; ',
        CASE
            WHEN connector_id_raw IS NOT NULL
                AND count(*) OVER (PARTITION BY batch_id, connector_id_raw) > 1
            THEN 'duplicate connector id'
        END,
        {{ error_if_invalid('connector_id_raw', 'char(22)', 'id') }},
        {{ error_if_invalid('station_name_raw', 'varchar(200)', 'commonName') }},
        CASE
            WHEN NOT {{ is_valid('lat_raw', 'decimal(8,5)') }}
                OR NOT {{ is_valid('lon_raw', 'decimal(8,5)') }}
            THEN 'invalid coordinates'
        END,
        {{ error_if_invalid('connector_type_raw', 'varchar(30)', 'ConnectorType') }},
        {{ error_if_invalid('parent_station_raw', 'varchar(40)', 'ParentStation') }},
        {{ error_if_invalid('power_kw_raw', 'int', 'Power') }},
        {{ error_if_invalid('status_raw', 'varchar(20)', 'Status') }},
        {{ error_if_null('updated_at', 'modified') }}
    ), '') AS error
FROM extracted e
)

SELECT
    batch_id,
    record_key,
    error,
    loaded_at,
    {{ cast_if_valid('connector_id_raw', 'char(22)') }} AS connector_id,
    {{ cast_if_valid('station_name_raw', 'varchar(200)') }} AS station_name,
    {{ cast_if_valid('lat_raw', 'decimal(8,5)') }} AS lat,
    {{ cast_if_valid('lon_raw', 'decimal(8,5)') }} AS lon,
    {{ cast_if_valid('connector_type_raw', 'varchar(30)') }} AS connector_type,
    {{ cast_if_valid('parent_station_raw', 'varchar(40)') }} AS parent_station,
    {{ cast_if_valid('power_kw_raw', 'int') }} AS power_kw,
    {{ cast_if_valid('status_raw', 'varchar(20)') }} AS status,
    {{ cast_if_valid('updated_at', 'timestamptz') }} AS updated_at
FROM validated

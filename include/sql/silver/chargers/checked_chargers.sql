DROP VIEW IF EXISTS silver.checked_chargers CASCADE;

CREATE VIEW silver.checked_chargers AS
WITH extracted AS (
SELECT
    r.batch_id,
    r.record_key,
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
FROM bronze.raw_tfl r
CROSS JOIN LATERAL (
    SELECT
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'ConnectorType'
        ) AS connector_type_raw,
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'ParentStation'
        ) AS parent_station_raw,
        replace(
            max(prop->>'value') FILTER (
                WHERE prop->>'key' = 'Power'
            ),
            'kW',
            ''
        ) AS power_kw_raw,
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'Status'
        ) AS status_raw,
        max(
            CASE
                WHEN silver.is_valid(prop->>'modified', 'timestamptz')
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
), validated AS (
SELECT
    e.*,
    NULLIF(CONCAT_WS('; ',
        CASE
            WHEN NOT silver.is_valid(connector_id_raw, 'char(22)')
            THEN 'invalid id'
        END,
        CASE
            WHEN connector_id_raw IS NOT NULL
                AND count(*) OVER (PARTITION BY batch_id, connector_id_raw) > 1
            THEN 'duplicate connector id'
        END,
        CASE
            WHEN NOT silver.is_valid(station_name_raw, 'varchar(200)')
            THEN 'invalid commonName'
        END,
        CASE
            WHEN NOT silver.is_valid(lat_raw, 'decimal(8,5)')
                OR NOT silver.is_valid(lon_raw, 'decimal(8,5)')
            THEN 'invalid coordinates'
        END,
        CASE
            WHEN NOT silver.is_valid(connector_type_raw, 'varchar(30)')
            THEN 'invalid ConnectorType'
        END,
        CASE
            WHEN NOT silver.is_valid(parent_station_raw, 'varchar(40)')
            THEN 'invalid ParentStation'
        END,
        CASE
            WHEN NOT silver.is_valid(power_kw_raw, 'int')
            THEN 'invalid Power'
        END,
        CASE
            WHEN NOT silver.is_valid(status_raw, 'varchar(20)')
            THEN 'invalid Status'
        END,
        CASE
            WHEN updated_at IS NULL
            THEN 'invalid modified'
        END
    ), '') AS error
FROM extracted e
) SELECT
    batch_id,
    record_key,
    error,
    CASE
        WHEN error IS NULL
        THEN connector_id_raw::char(22)
    END AS connector_id,
    CASE
        WHEN error IS NULL
        THEN station_name_raw::varchar(200)
    END AS station_name,
    CASE
        WHEN error IS NULL
        THEN lat_raw::decimal(8,5)
    END AS lat,
    CASE
        WHEN error IS NULL
        THEN lon_raw::decimal(8,5)
    END AS lon,
    CASE
        WHEN error IS NULL
        THEN connector_type_raw::varchar(30)
    END AS connector_type,
    CASE
        WHEN error IS NULL
        THEN parent_station_raw::varchar(40)
    END AS parent_station,
    CASE
        WHEN error IS NULL
        THEN power_kw_raw::int
    END AS power_kw,
    CASE
        WHEN error IS NULL
        THEN status_raw::varchar(20)
    END AS status,
    CASE
        WHEN error IS NULL
        THEN updated_at
    END AS updated_at
FROM validated;

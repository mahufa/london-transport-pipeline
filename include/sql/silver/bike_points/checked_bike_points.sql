DROP VIEW IF EXISTS silver.checked_bike_points CASCADE;

CREATE VIEW silver.checked_bike_points AS
WITH extracted AS (
SELECT
    r.batch_id,
    r.record_key,
    replace(
        r.payload->>'id',
        'BikePoints_',
        ''
    ) AS bike_point_id_raw,
    r.payload->>'commonName' AS common_name_raw,
    r.payload->>'lat' AS lat_raw,
    r.payload->>'lon' AS lon_raw,
    p.*
FROM bronze.raw_tfl r
CROSS JOIN LATERAL (
    SELECT
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'NbBikes'
        ) AS nb_bikes_raw,
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'NbDocks'
        ) AS nb_docks_raw,
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'NbEBikes'
        ) AS nb_e_bikes_raw,
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'NbEmptyDocks'
        ) AS nb_empty_docks_raw,
        max(prop->>'value') FILTER (
            WHERE prop->>'key' = 'NbStandardBikes'
        ) AS nb_standard_bikes_raw,
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
WHERE r.source = 'bike_points'
), validated AS (
SELECT
    e.*,
    NULLIF(CONCAT_WS('; ',
        CASE
            WHEN NOT silver.is_valid(bike_point_id_raw, 'int')
            THEN 'invalid id'
        END,
        CASE
            WHEN NOT silver.is_valid(common_name_raw, 'varchar(100)')
            THEN 'invalid commonName'
        END,
        CASE
            WHEN NOT silver.is_valid(lat_raw, 'decimal(8,5)')
                OR NOT silver.is_valid(lon_raw, 'decimal(8,5)')
            THEN 'invalid coordinates'
        END,
        CASE
            WHEN NOT silver.is_valid(nb_bikes_raw, 'int')
            THEN 'invalid NbBikes'
        END,
        CASE
            WHEN NOT silver.is_valid(nb_docks_raw, 'int')
            THEN 'invalid NbDocks'
        END,
        CASE
            WHEN NOT silver.is_valid(nb_e_bikes_raw, 'int')
            THEN 'invalid NbEBikes'
        END,
        CASE
            WHEN NOT silver.is_valid(nb_empty_docks_raw, 'int')
            THEN 'invalid NbEmptyDocks'
        END,
        CASE
            WHEN NOT silver.is_valid(nb_standard_bikes_raw, 'int')
            THEN 'invalid NbStandardBikes'
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
        THEN bike_point_id_raw::int
    END AS bike_point_id,
    CASE
        WHEN error IS NULL
        THEN common_name_raw::varchar(100)
    END AS common_name,
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
        THEN nb_bikes_raw::int
    END AS nb_bikes,
    CASE
        WHEN error IS NULL
        THEN nb_docks_raw::int
    END AS nb_docks,
    CASE
        WHEN error IS NULL
        THEN nb_e_bikes_raw::int
    END AS nb_e_bikes,
    CASE
        WHEN error IS NULL
        THEN nb_empty_docks_raw::int
    END AS nb_empty_docks,
    CASE
        WHEN error IS NULL
        THEN nb_standard_bikes_raw::int
    END AS nb_standard_bikes,
    CASE
        WHEN error IS NULL
        THEN updated_at
    END AS updated_at
FROM validated;

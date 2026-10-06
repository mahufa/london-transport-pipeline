--validation helper:
-- pg_input_is_valid() returns NULL for NULL input,
-- so missing values would pass as valid
CREATE OR REPLACE FUNCTION silver.is_valid(
    value text,
    type_name text)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(pg_input_is_valid(value, type_name), false)
$$;


--bike_points:
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


CREATE VIEW silver.stg_bike_points AS
SELECT
    bike_point_id,
    common_name,
    lat,
    lon,
    nb_bikes,
    nb_docks,
    nb_e_bikes,
    nb_empty_docks,
    nb_standard_bikes,
    updated_at,
    batch_id
FROM silver.checked_bike_points
WHERE error IS NULL;




--chargers:
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


CREATE VIEW silver.stg_chargers AS
SELECT
    connector_id,
    station_name,
    lat,
    lon,
    connector_type,
    parent_station,
    power_kw,
    status,
    updated_at,
    batch_id
FROM silver.checked_chargers
WHERE error IS NULL;




--roads:
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

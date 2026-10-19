WITH connectors AS (
SELECT
    connector_id,
    power_kw,
    connector_type,
    row_number() OVER (
        PARTITION BY connector_id
        ORDER BY loaded_at DESC
    ) AS rn
FROM {{ ref('int_validated_chargers') }}
)

SELECT
    connector_id,
    power_kw,
    connector_type
FROM connectors
WHERE rn = 1

WITH disruptions AS (
SELECT
    disruption_id,
    category,
    subcategory,
    severity,
    row_number() OVER (
        PARTITION BY disruption_id
        ORDER BY loaded_at DESC
    ) AS rn
FROM {{ ref('int_validated_roads') }}
)

SELECT
    disruption_id,
    category,
    subcategory,
    severity
FROM disruptions
WHERE rn = 1

SELECT
    source,
    batch_id,
    'ingest' AS stage,
    record,
    error,
    rejected_at
FROM {{ source('bronze', 'rejected_records') }}
UNION ALL
SELECT
    source,
    batch_id,
    stage,
    record,
    error,
    rejected_at
FROM {{ ref('int_rejected_bike_points') }}
UNION ALL
SELECT
    source,
    batch_id,
    stage,
    record,
    error,
    rejected_at
FROM {{ ref('int_rejected_chargers') }}
UNION ALL
SELECT
    source,
    batch_id,
    stage,
    record,
    error,
    rejected_at
FROM {{ ref('int_rejected_roads') }}

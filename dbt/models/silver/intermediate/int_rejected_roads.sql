SELECT
    r.source,
    r.batch_id,
    'silver' AS stage,
    r.payload::text AS record,
    stg.error
FROM {{ ref('stg_roads') }} stg
JOIN {{ source('bronze', 'raw_tfl') }} r
    ON r.source = 'roads'
    AND r.batch_id = stg.batch_id
    AND r.record_key = stg.record_key
WHERE stg.error IS NOT NULL

BEGIN;

DELETE FROM bronze.rejected_records
WHERE source = 'roads'
    AND batch_id = %(batch_id)s
    AND stage = 'silver';


INSERT INTO bronze.rejected_records(
    source,
    batch_id,
    stage,
    record,
    error
)
SELECT
    r.source,
    r.batch_id,
    'silver',
    r.payload::text,
    c.error
FROM silver.checked_roads c
JOIN bronze.raw_tfl r
    ON r.source = 'roads'
    AND r.batch_id = c.batch_id
    AND r.record_key = c.record_key
WHERE c.batch_id = %(batch_id)s
    AND c.error IS NOT NULL;

COMMIT;

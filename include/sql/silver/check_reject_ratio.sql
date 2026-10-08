-- one boolean row, as expected by SQLCheckOperator: false fails the task
WITH counts AS (
SELECT
    (
        SELECT count(*)
        FROM bronze.rejected_records
        WHERE source = %(source)s
            AND batch_id = %(batch_id)s
    ) AS rejected,
    (
        SELECT count(*)
        FROM bronze.raw_tfl
        WHERE source = %(source)s
            AND batch_id = %(batch_id)s
    ) + (
        SELECT count(*)
        FROM bronze.rejected_records
        WHERE source = %(source)s
            AND batch_id = %(batch_id)s
            AND stage = 'ingest'
    ) AS total
)
SELECT
    COALESCE(
        rejected::numeric / NULLIF(total, 0),
        0
    ) <= %(max_reject_ratio)s AS reject_ratio_ok
FROM counts;

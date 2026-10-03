CREATE TEMP TABLE ranked ON COMMIT DROP AS
SELECT
    ordinal,
    record_key,
    payload,
    row_number() OVER (
        PARTITION BY record_key
        ORDER BY ordinal
    ) AS rn
FROM landing;

INSERT INTO bronze.raw_tfl (
    source,
    batch_id,
    record_key,
    payload
)
SELECT
    %(source)s,
    %(batch_id)s,
    record_key,
    payload
FROM ranked
WHERE rn = 1;

INSERT INTO bronze.rejected_records (
    source,
    batch_id,
    stage,
    record,
    error)
SELECT
    %(source)s,
    %(batch_id)s,
    'ingest',
    payload::text,
'duplicate record key'
FROM ranked
WHERE rn > 1;

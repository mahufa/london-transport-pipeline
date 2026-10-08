CREATE TEMP TABLE landing (
    ordinal bigint,
    record_key text,
    payload jsonb
) ON COMMIT DROP;

DELETE FROM bronze.raw_tfl
WHERE source = %(source)s
    AND batch_id = %(batch_id)s;

DELETE FROM bronze.rejected_records
WHERE source = %(source)s
    AND batch_id = %(batch_id)s AND stage = 'ingest';

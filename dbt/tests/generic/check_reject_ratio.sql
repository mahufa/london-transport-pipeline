{% test check_reject_ratio(model, source_name, max_reject_ratio=none) -%}

{{ config(severity='warn') }}

{% if max_reject_ratio is none %}
{%- set max_reject_ratio = var('max_reject_ratio') -%}
{% endif %}
{%- set batch_id = var('batch_id', none) -%}

-- warn-only: invalid rows are already filtered out, so a high ratio is an alert, not a gate
-- model holds only valid rows, so rejects = raw + ingest rejects - valid
WITH records AS (
    SELECT batch_id, loaded_at
    FROM {{ source('bronze', 'raw_tfl') }}
    WHERE source = '{{ source_name }}'
    UNION ALL
    SELECT batch_id, rejected_at
    FROM {{ source('bronze', 'rejected_records') }}
    WHERE source = '{{ source_name }}'
),

-- batches within the lookback of the newest one, or only the one passed for backfills
batches AS (
    SELECT
        batch_id,
        count(*) AS total
    FROM records
    {% if batch_id %}
    WHERE batch_id = '{{ batch_id }}'
    {% endif %}
    GROUP BY batch_id
    {% if not batch_id %}
    HAVING max(loaded_at) > (SELECT max(loaded_at) FROM records) - interval '{{ var('incremental_lookback') }}'
    {% endif %}
),

valid AS (
    SELECT
        batch_id,
        count(*) AS valid
    FROM {{ model }}
    WHERE batch_id IN (SELECT batch_id FROM batches)
    GROUP BY batch_id
)

SELECT
    b.batch_id,
    b.total,
    COALESCE(v.valid, 0) AS valid
FROM batches b
LEFT JOIN valid v USING (batch_id)
WHERE (b.total - COALESCE(v.valid, 0))::numeric / b.total > {{ max_reject_ratio }}

{%- endtest %}

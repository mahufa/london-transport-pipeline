{% test check_reject_ratio(model, source_name, max_reject_ratio=none) -%}

{{ config(severity='warn') }}

{% if max_reject_ratio is none %}
{%- set max_reject_ratio = var('max_reject_ratio') -%}
{% endif %}
{%- set batch_id = var('batch_id', none) -%}

-- warn-only: invalid rows are already filtered out, so a high ratio is an alert, not a gate
-- model holds only valid rows, so rejects = raw + ingest rejects - valid
WITH records AS (
    SELECT batch_id
    FROM {{ source('bronze', 'raw_tfl') }}
    WHERE source = '{{ source_name }}'
    UNION ALL
    SELECT batch_id
    FROM {{ source('bronze', 'rejected_records') }}
    WHERE source = '{{ source_name }}'
),

latest AS (
    {% if batch_id %}
    SELECT '{{ batch_id }}'::text AS batch_id
    {% else %}
    SELECT max(batch_id) AS batch_id
    FROM records
    {% endif %}
),

counts AS (
    SELECT
        (
            SELECT count(*)
            FROM records
            WHERE batch_id = (SELECT batch_id FROM latest)
        ) AS total,
        (
            SELECT count(*)
            FROM {{ model }}
            WHERE batch_id = (SELECT batch_id FROM latest)
        ) AS valid
)

SELECT
    1
FROM counts
WHERE COALESCE((total - valid)::numeric / NULLIF(total, 0), 0) > {{ max_reject_ratio }}

{%- endtest %}

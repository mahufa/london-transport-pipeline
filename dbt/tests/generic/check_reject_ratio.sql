{% test check_reject_ratio(model, source_name, max_reject_ratio=none) -%}

{% if max_reject_ratio is none %}
{%- set max_reject_ratio = var('max_reject_ratio') -%}
{% endif %}
{%- set batch_id = var('batch_id', none) -%}

WITH latest AS (
    {% if batch_id %}
    SELECT '{{ batch_id }}'::text AS batch_id
    {% else %}
    SELECT max(batch_id) AS batch_id
    FROM (
        SELECT batch_id FROM {{ source('bronze', 'raw_tfl') }} rt
        WHERE rt.source = '{{ source_name }}'
        UNION ALL
        SELECT batch_id
        FROM {{ source('bronze', 'rejected_records') }} rr
        WHERE rr.source = '{{ source_name }}'
    ) batch
    {% endif %}
),

counts AS (
    SELECT
        (
            SELECT count(*)
            FROM {{ model }}
            WHERE error IS NOT NULL
                AND batch_id = (SELECT batch_id FROM latest)
        ) + (
            SELECT count(*)
            FROM {{ source('bronze', 'rejected_records') }} rr
            WHERE rr.source = '{{ source_name }}'
                AND batch_id = (SELECT batch_id FROM latest)
        ) AS rejected,
        (
            SELECT count(*)
            FROM {{ model }}
            WHERE batch_id = (SELECT batch_id FROM latest)
        ) + (
            SELECT count(*)
            FROM {{ source('bronze', 'rejected_records') }} rr
            WHERE rr.source = '{{ source_name }}'
                AND batch_id = (SELECT batch_id FROM latest)
        ) AS total
)

SELECT
    1
FROM counts
WHERE COALESCE(
        rejected::numeric / NULLIF(total, 0),
        0
    ) > {{ max_reject_ratio }}

{%- endtest %}
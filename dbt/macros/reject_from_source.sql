{% macro reject_from_source(source_name) -%}
    SELECT
        r.source,
        r.batch_id,
        'silver' AS stage,
        r.payload::text AS record,
        r.loaded_at AS rejected_at,
        stg.error
    FROM {{ ref('stg_' ~ source_name) }} stg
    JOIN {{ source('bronze', 'raw_tfl') }} r
        ON r.source = '{{ source_name }}'
        AND r.batch_id = stg.batch_id
        AND r.record_key = stg.record_key
    WHERE stg.error IS NOT NULL
{%- endmacro %}

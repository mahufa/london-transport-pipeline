{% for source_name in ['bike_points', 'chargers', 'roads'] %}
    {{ reject_from_source(source_name) }}
    UNION ALL
{% endfor %}
SELECT
    source,
    batch_id,
    'ingest' AS stage,
    record,
    rejected_at,
    error
FROM {{ source('bronze', 'rejected_records') }}

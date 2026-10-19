{{ config(unique_key=['disruption_id', 'disrupted_segment_id']) }}
WITH disrupted AS (
SELECT
    disruption_id,
    disrupted_segment_id,
    start_date_time,
    end_date_time,
    closure,
    directions,
    batch_id,
    loaded_at,
    row_number() OVER (
        PARTITION BY disruption_id, disrupted_segment_id
        ORDER BY loaded_at DESC
    ) AS rn
FROM {{ ref('int_validated_roads') }}
{% if is_incremental() %}
WHERE {{ incremental_since('loaded_at') }}
{% endif %}
)

SELECT
    disruption_id,
    disrupted_segment_id,
    start_date_time,
    end_date_time,
    closure,
    directions,
    batch_id,
    loaded_at
FROM disrupted
WHERE rn = 1

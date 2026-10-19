{{ config(
    incremental_strategy='append',
    indexes=[{'columns': ['connector_id', 'updated_at'], 'unique': True}]
) }}
WITH changes AS (
SELECT
    parent_station AS charging_station_id,
    connector_id,
    updated_at,
    status,
    batch_id,
    loaded_at,
    row_number() OVER (
        PARTITION BY connector_id, updated_at
        ORDER BY loaded_at ASC
    ) AS rn
FROM {{ ref('int_validated_chargers') }} v
{% if is_incremental() %}
WHERE {{ incremental_since('loaded_at') }}
    AND {{ not_in_this(['connector_id', 'updated_at']) }}
{% endif %}
)

SELECT
    charging_station_id,
    connector_id,
    updated_at,
    status,
    batch_id,
    loaded_at
FROM changes
WHERE rn = 1

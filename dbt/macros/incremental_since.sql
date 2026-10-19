{% macro incremental_since(column, lookback=none) -%}
    {%- set lookback = lookback or var('incremental_lookback') -%}
    {{ column }} > (
        SELECT coalesce(max({{ column }}), '-infinity') - interval '{{ lookback }}'
        FROM {{ this }}
    )
{%- endmacro %}

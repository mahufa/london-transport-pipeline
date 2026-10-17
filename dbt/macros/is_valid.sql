{% macro is_valid(value, type) -%}
    coalesce(pg_input_is_valid({{ value }}, '{{ type }}'), false)
{%- endmacro %}

{% macro error_if_invalid(value, type, col_name) -%}
    CASE
        WHEN NOT {{ is_valid(value, type) }}
        THEN 'invalid {{ col_name }}'
    END
{%- endmacro %}

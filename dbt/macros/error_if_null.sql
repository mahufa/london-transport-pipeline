{% macro error_if_null(value, col_name) -%}
    CASE
        WHEN {{ value }} IS NULL
        THEN 'invalid {{ col_name }}'
    END
{%- endmacro %}

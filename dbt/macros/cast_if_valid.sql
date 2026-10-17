{% macro cast_if_valid(value, type) -%}
    CASE
        WHEN error IS NULL
        THEN {{ value }}::{{ type }}
    END
{%- endmacro %}

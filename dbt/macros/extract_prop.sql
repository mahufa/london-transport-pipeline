{% macro extract_prop(prop_name) -%}
    max(prop->>'value') FILTER (
        WHERE prop->>'key' = '{{ prop_name }}'
    )
{%- endmacro %}

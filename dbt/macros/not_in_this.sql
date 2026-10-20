{% macro not_in_this(keys, alias='v') -%}
    NOT EXISTS (
        SELECT 1
        FROM {{ this }} t
        WHERE
        {%- for key in keys %}
            t.{{ key }} = {{ alias }}.{{ key }} {{- " AND" if not loop.last }}
        {%- endfor %}
    )
{%- endmacro %}

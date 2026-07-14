{#
    generate_period_surrogate_key
    ------------------------------
    Builds a stable hashed surrogate key from a list of column names.
    Written as plain BigQuery SQL (MD5 + CONCAT) so the project has zero
    external package dependencies (no dbt_utils install needed).

    Usage:
        {{ generate_period_surrogate_key(['period_label', 'period_type', 'fiscal_year']) }} as period_key
#}
{% macro generate_period_surrogate_key(columns) -%}
    TO_HEX(MD5(CONCAT(
        {%- for col in columns %}
        COALESCE(CAST({{ col }} AS STRING), '_null_')
        {%- if not loop.last %},'~',{% endif %}
        {%- endfor %}
    )))
{%- endmacro %}
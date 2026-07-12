{#
    yoy_growth
    ----------
    Year-over-year growth % for a metric, computed within a partition
    (e.g. FY periods vs FY periods, HY vs HY — never mixed) ordered by
    fiscal_year. Uses LAG + SAFE_DIVIDE so the first period in a partition
    (no prior year) returns NULL rather than erroring.

    Usage:
        {{ yoy_growth('mpesa_revenue_kes_bn', 'period_type', 'fiscal_year') }} as mpesa_revenue_yoy_pct
#}
{% macro yoy_growth(column, partition_by, order_by) -%}
    ROUND(
        SAFE_DIVIDE(
            {{ column }} - LAG({{ column }}) OVER (PARTITION BY {{ partition_by }} ORDER BY {{ order_by }}),
            LAG({{ column }}) OVER (PARTITION BY {{ partition_by }} ORDER BY {{ order_by }})
        ) * 100,
        2
    )
{%- endmacro %}
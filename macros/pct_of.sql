{#
    pct_of
    ------
    Returns numerator as a percentage of denominator, rounded to 2dp.
    Uses BigQuery's SAFE_DIVIDE so nulls / zero denominators return NULL
    instead of throwing (several early-year source rows have gaps).

    Usage:
        {{ pct_of('mpesa_kes_bn', 'total_service_revenue_kes_bn') }} as mpesa_pct_of_total
#}
{% macro pct_of(numerator, denominator) -%}
    ROUND(SAFE_DIVIDE({{ numerator }}, {{ denominator }}) * 100, 2)
{%- endmacro %}
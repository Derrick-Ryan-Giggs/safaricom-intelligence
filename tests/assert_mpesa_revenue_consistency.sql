-- Fails if the M-PESA revenue figure reported in mpesa_metrics (sourced from
-- Section 4A of the booklet) doesn't match the M-PESA line in
-- revenue_segments (sourced from the Kenya income statement, Section 1b/4B)
-- for the same period. Both should tie out since they describe the same
-- underlying revenue line, just pulled from different pages of the booklet.

select
    m.period_label,
    m.period_type,
    m.fiscal_year,
    m.mpesa_revenue_kes_bn as mpesa_metrics_revenue,
    s.mpesa_kes_bn         as revenue_segments_mpesa,
    round(abs(m.mpesa_revenue_kes_bn - s.mpesa_kes_bn), 2) as variance_kes_bn

from {{ ref('stg_mpesa_metrics') }} m
inner join {{ ref('stg_revenue_segments') }} s
    on m.period_label = s.period_label
    and m.period_type = s.period_type
    and m.fiscal_year = s.fiscal_year

where abs(m.mpesa_revenue_kes_bn - s.mpesa_kes_bn) > 0.5
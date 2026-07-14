-- Fails if connectivity_total + mpesa + fixed_service_iot diverges from the
-- reported total_service_revenue_kes_bn by more than 1 KES bn. A small
-- tolerance is allowed because the source booklets round each line
-- independently, so the reported total can be off by rounding drift.

select
    period_label,
    period_type,
    fiscal_year,
    total_service_revenue_kes_bn,
    computed_total_service_revenue_kes_bn,
    round(abs(total_service_revenue_kes_bn - computed_total_service_revenue_kes_bn), 2) as variance_kes_bn

from {{ ref('stg_revenue_segments') }}

where abs(total_service_revenue_kes_bn - computed_total_service_revenue_kes_bn) > 1
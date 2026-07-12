with source as (

    select * from {{ source('raw', 'company_overview') }}

),

renamed as (

    select
        {{ generate_period_surrogate_key(['period_label', 'period_type', 'fiscal_year']) }} as period_key,
        period_label,
        period_type,
        fiscal_year,
        period_end_date,
        total_revenue_kes_bn,
        service_revenue_kes_bn,
        ebit_kes_bn,
        net_income_kes_bn,
        active_customers_mn,
        capex_kes_bn,

        -- derived
        {{ pct_of('ebit_kes_bn', 'service_revenue_kes_bn') }}       as ebit_margin_pct,
        {{ pct_of('net_income_kes_bn', 'service_revenue_kes_bn') }} as net_margin_pct,
        {{ pct_of('capex_kes_bn', 'service_revenue_kes_bn') }}      as capex_intensity_pct

    from source

)

select * from renamed
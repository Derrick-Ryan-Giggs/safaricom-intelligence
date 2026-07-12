with source as (

    select * from {{ source('raw', 'kenya_ethiopia') }}

),

renamed as (

    select
        {{ generate_period_surrogate_key(['period_label', 'period_type', 'fiscal_year', 'geography']) }} as period_geo_key,
        period_label,
        period_type,
        fiscal_year,
        period_end_date,
        geography,
        service_revenue_kes_bn,
        active_customers_1m_mn,
        active_customers_3m_mn,
        ebit_kes_bn,
        capex_kes_bn,

        -- derived
        {{ pct_of('ebit_kes_bn', 'service_revenue_kes_bn') }} as ebit_margin_pct

    from source

)

select * from renamed
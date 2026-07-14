with ke_et as (

    select * from {{ ref('stg_kenya_ethiopia') }}

),

pivoted as (

    select
        period_label,
        period_type,
        fiscal_year,
        period_end_date,
        max(case when geography = 'KE' then service_revenue_kes_bn end) as ke_service_revenue_kes_bn,
        max(case when geography = 'ET' then service_revenue_kes_bn end) as et_service_revenue_kes_bn,
        max(case when geography = 'KE' then ebit_kes_bn end)            as ke_ebit_kes_bn,
        max(case when geography = 'ET' then ebit_kes_bn end)            as et_ebit_kes_bn,
        max(case when geography = 'KE' then active_customers_1m_mn end) as ke_active_customers_1m_mn,
        max(case when geography = 'ET' then active_customers_1m_mn end) as et_active_customers_1m_mn,
        max(case when geography = 'KE' then active_customers_3m_mn end) as ke_active_customers_3m_mn,
        max(case when geography = 'ET' then active_customers_3m_mn end) as et_active_customers_3m_mn,
        max(case when geography = 'KE' then capex_kes_bn end)           as ke_capex_kes_bn,
        max(case when geography = 'ET' then capex_kes_bn end)           as et_capex_kes_bn
    from ke_et
    group by 1, 2, 3, 4

),

with_metrics as (

    select
        *,
        {{ pct_of('ke_ebit_kes_bn', 'ke_service_revenue_kes_bn') }} as ke_ebit_margin_pct,
        {{ pct_of('et_ebit_kes_bn', 'et_service_revenue_kes_bn') }} as et_ebit_margin_pct,

        {{ yoy_growth('ke_service_revenue_kes_bn', 'period_type', 'fiscal_year') }} as ke_revenue_yoy_pct,
        {{ yoy_growth('et_service_revenue_kes_bn', 'period_type', 'fiscal_year') }} as et_revenue_yoy_pct,

        -- positive value = loss narrowing year-on-year (ET EBIT moving toward zero)
        round(
            et_ebit_kes_bn - lag(et_ebit_kes_bn) over (partition by period_type order by fiscal_year),
            2
        ) as et_ebit_loss_narrowing_kes_bn

    from pivoted

)

select * from with_metrics
order by period_type, fiscal_year
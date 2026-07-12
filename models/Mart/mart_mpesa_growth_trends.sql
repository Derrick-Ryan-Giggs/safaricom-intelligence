with mpesa as (

    select * from {{ ref('stg_mpesa_metrics') }}

),

segments as (

    select
        period_label,
        period_type,
        fiscal_year,
        mpesa_pct_of_total,
        total_service_revenue_kes_bn
    from {{ ref('stg_revenue_segments') }}

),

joined as (

    select
        m.period_key,
        m.period_label,
        m.period_type,
        m.fiscal_year,
        m.period_end_date,
        m.mpesa_revenue_kes_bn,
        m.mpesa_txn_value_kes_bn,
        m.mpesa_txn_volume_mn,
        m.avg_txn_value_kes,
        m.mpesa_customers_1m_mn,
        m.merchants_mn,
        m.business_payments_kes_bn,
        m.mpesa_global_kes_bn,
        m.merchant_overdraft_customers,
        s.mpesa_pct_of_total,
        s.total_service_revenue_kes_bn
    from mpesa m
    left join segments s
        on m.period_label = s.period_label
        and m.period_type = s.period_type
        and m.fiscal_year = s.fiscal_year

),

with_growth as (

    select
        *,
        {{ yoy_growth('mpesa_revenue_kes_bn', 'period_type', 'fiscal_year') }}    as mpesa_revenue_yoy_pct,
        {{ yoy_growth('mpesa_customers_1m_mn', 'period_type', 'fiscal_year') }}   as mpesa_customers_yoy_pct,
        {{ yoy_growth('merchants_mn', 'period_type', 'fiscal_year') }}            as merchants_yoy_pct,
        {{ yoy_growth('business_payments_kes_bn', 'period_type', 'fiscal_year') }} as business_payments_yoy_pct,

        -- 3-period trailing average (3 FY periods, or 3 HY periods within their own partition)
        round(
            avg(mpesa_revenue_kes_bn) over (
                partition by period_type
                order by fiscal_year
                rows between 2 preceding and current row
            ), 2
        ) as mpesa_revenue_3yr_rolling_avg_kes_bn

    from joined

)

select * from with_growth
order by period_type, fiscal_year
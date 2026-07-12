with segments as (

    select * from {{ ref('stg_revenue_segments') }}

),

mix as (

    select
        period_key,
        period_label,
        period_type,
        fiscal_year,
        period_end_date,
        voice_kes_bn,
        mobile_data_kes_bn,
        messaging_kes_bn,
        mobile_incoming_kes_bn,
        other_mobile_service_kes_bn,
        mpesa_kes_bn,
        fixed_service_iot_kes_bn,
        connectivity_total_kes_bn,
        total_service_revenue_kes_bn,

        -- share of total service revenue, per segment
        {{ pct_of('voice_kes_bn', 'total_service_revenue_kes_bn') }}            as voice_share_pct,
        {{ pct_of('mobile_data_kes_bn', 'total_service_revenue_kes_bn') }}      as data_share_pct,
        {{ pct_of('messaging_kes_bn', 'total_service_revenue_kes_bn') }}        as messaging_share_pct,
        {{ pct_of('mpesa_kes_bn', 'total_service_revenue_kes_bn') }}            as mpesa_share_pct,
        {{ pct_of('fixed_service_iot_kes_bn', 'total_service_revenue_kes_bn') }} as fixed_iot_share_pct,

        -- YoY growth per segment, within FY-vs-FY / HY-vs-HY partitions
        {{ yoy_growth('voice_kes_bn', 'period_type', 'fiscal_year') }}       as voice_yoy_pct,
        {{ yoy_growth('mobile_data_kes_bn', 'period_type', 'fiscal_year') }} as data_yoy_pct,
        {{ yoy_growth('messaging_kes_bn', 'period_type', 'fiscal_year') }}   as messaging_yoy_pct,
        {{ yoy_growth('mpesa_kes_bn', 'period_type', 'fiscal_year') }}       as mpesa_yoy_pct,
        {{ yoy_growth('fixed_service_iot_kes_bn', 'period_type', 'fiscal_year') }} as fixed_iot_yoy_pct

    from segments

)

select * from mix
order by period_type, fiscal_year
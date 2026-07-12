with source as (

    select * from {{ source('raw', 'revenue_segments') }}

),

renamed as (

    select
        {{ generate_period_surrogate_key(['period_label', 'period_type', 'fiscal_year']) }} as period_key,
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

        -- reconciliation column, checked by tests/assert_revenue_segments_reconcile.sql
        connectivity_total_kes_bn + mpesa_kes_bn + fixed_service_iot_kes_bn as computed_total_service_revenue_kes_bn,

        -- derived shares
        {{ pct_of('mpesa_kes_bn', 'total_service_revenue_kes_bn') }}             as mpesa_pct_of_total,
        {{ pct_of('connectivity_total_kes_bn', 'total_service_revenue_kes_bn') }} as connectivity_pct_of_total,
        {{ pct_of('fixed_service_iot_kes_bn', 'total_service_revenue_kes_bn') }}  as fixed_iot_pct_of_total,
        {{ pct_of('voice_kes_bn', 'connectivity_total_kes_bn') }}                 as voice_pct_of_connectivity,
        {{ pct_of('mobile_data_kes_bn', 'connectivity_total_kes_bn') }}          as data_pct_of_connectivity

    from source

)

select * from renamed
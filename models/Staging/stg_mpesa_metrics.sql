with source as (

    select * from {{ source('raw', 'mpesa_metrics') }}

),

renamed as (

    select
        {{ generate_period_surrogate_key(['period_label', 'period_type', 'fiscal_year']) }} as period_key,
        period_label,
        period_type,
        fiscal_year,
        period_end_date,
        mpesa_revenue_kes_bn,
        mpesa_txn_value_kes_bn,
        mpesa_txn_volume_mn,
        mpesa_customers_1m_mn,
        merchants_mn,
        business_payments_kes_bn,
        mpesa_global_kes_bn,
        merchant_overdraft_customers,

        -- derived: average value per M-PESA transaction, in KES
        -- (mpesa_txn_value_kes_bn * 1000) converts billions -> millions of KES,
        -- so millions-of-KES / millions-of-transactions = KES per transaction
        ROUND(SAFE_DIVIDE(mpesa_txn_value_kes_bn * 1000, mpesa_txn_volume_mn), 2) as avg_txn_value_kes

    from source

)

select * from renamed
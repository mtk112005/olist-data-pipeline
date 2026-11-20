select
    order_id,
    sum(payment_value) as payment_value,
    count(*) as payment_count,
    max(payment_installments) as max_installments,
    -- main payment type = the one carrying the most value
    (array_agg(payment_type order by payment_value desc))[1] as main_payment_type,
    bool_or(payment_type = 'voucher') as used_voucher
from {{ ref('stg_order_payments') }}
group by 1

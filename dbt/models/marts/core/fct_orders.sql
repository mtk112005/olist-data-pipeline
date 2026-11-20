{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail',
    indexes=[{'columns': ['purchase_date']}, {'columns': ['customer_unique_id']}]
  )
}}

-- One row per order. Incremental: each Airflow run only processes orders whose
-- raw rows were loaded after the latest load already in this table.
with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    o.order_id::text as order_id,
    o.customer_id::text as customer_id,
    c.customer_unique_id::text as customer_unique_id,
    o.order_status::text as order_status,
    o.purchase_date::date as purchase_date,
    o.order_purchase_timestamp::timestamp as order_purchase_timestamp,
    o.order_approved_at::timestamp as order_approved_at,
    o.order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
    o.order_delivered_customer_date::timestamp as order_delivered_customer_date,
    o.order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
    coalesce(i.item_count, 0)::integer as item_count,
    coalesce(i.seller_count, 0)::integer as seller_count,
    coalesce(i.items_value, 0)::numeric(12,2) as items_value,
    coalesce(i.freight_value, 0)::numeric(12,2) as freight_value,
    (coalesce(i.items_value, 0) + coalesce(i.freight_value, 0))::numeric(12,2) as order_value,
    coalesce(p.payment_value, 0)::numeric(12,2) as payment_value,
    p.main_payment_type::text as main_payment_type,
    p.max_installments::integer as max_installments,
    coalesce(p.used_voucher, false)::boolean as used_voucher,
    r.review_score::integer as review_score,
    r.has_comment::boolean as review_has_comment,
    (o.order_status = 'delivered' and o.order_delivered_customer_date is not null)::boolean as is_delivered,
    round(extract(epoch from o.order_delivered_customer_date - o.order_purchase_timestamp) / 86400.0, 2)::numeric(8,2)
        as delivery_days,
    round(extract(epoch from o.order_estimated_delivery_date - o.order_purchase_timestamp) / 86400.0, 2)::numeric(8,2)
        as promised_days,
    (o.order_delivered_customer_date::date - o.order_estimated_delivery_date::date)::integer as delay_days,
    coalesce(o.order_delivered_customer_date::date
             > o.order_estimated_delivery_date::date + {{ var('late_tolerance_days') }}, false)::boolean as is_late,
    coalesce(o.order_delivered_carrier_date < o.order_purchase_timestamp, false)::boolean as has_timeline_anomaly,
    o._loaded_at::timestamptz as _loaded_at
from orders o
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('int_order_items_agg') }} i on o.order_id = i.order_id
left join {{ ref('int_order_payments') }} p on o.order_id = p.order_id
left join {{ ref('stg_order_reviews') }} r on o.order_id = r.order_id

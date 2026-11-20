-- One row per real customer (customer_unique_id), not per order-level customer_id.
with orders as (
    select * from {{ ref('fct_orders') }}
),

latest_address as (
    select distinct on (c.customer_unique_id)
        c.customer_unique_id, c.zip_code_prefix, c.city, c.state
    from {{ ref('stg_customers') }} c
    join orders o on o.customer_id = c.customer_id
    order by c.customer_unique_id, o.order_purchase_timestamp desc
)

select
    a.customer_unique_id,
    a.zip_code_prefix,
    a.city,
    a.state,
    min(o.purchase_date) as first_order_date,
    max(o.purchase_date) as last_order_date,
    count(*) as order_count,
    count(*) filter (where o.is_delivered) as delivered_order_count,
    sum(o.payment_value)::numeric(12,2) as lifetime_value,
    avg(o.review_score)::numeric(4,2) as avg_review_score,
    count(*) > 1 as is_repeat_customer
from orders o
join latest_address a on o.customer_unique_id = a.customer_unique_id
group by 1, 2, 3, 4

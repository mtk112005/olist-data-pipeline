-- Monthly acquisition cohorts and how many customers come back.
with orders as (
    select customer_unique_id, date_trunc('month', purchase_date)::date as order_month
    from {{ ref('fct_orders') }}
    where order_status not in ('canceled', 'unavailable')
),

first_month as (
    select customer_unique_id, min(order_month) as cohort_month
    from orders
    group by 1
),

activity as (
    select distinct
        f.cohort_month,
        o.customer_unique_id,
        (extract(year from age(o.order_month, f.cohort_month)) * 12
         + extract(month from age(o.order_month, f.cohort_month)))::int as months_since_first
    from orders o
    join first_month f using (customer_unique_id)
),

sizes as (
    select cohort_month, count(*) as cohort_size from first_month group by 1
)

select
    a.cohort_month,
    a.months_since_first,
    s.cohort_size,
    count(*) as active_customers,
    round(count(*)::numeric / s.cohort_size, 4) as retention_rate
from activity a
join sizes s using (cohort_month)
group by 1, 2, 3

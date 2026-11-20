-- Does lateness hurt review scores? Monthly, by customer state.
select
    date_trunc('month', purchase_date)::date as month,
    c.state as customer_state,
    count(*) as delivered_orders,
    round(avg(o.delivery_days), 2) as avg_delivery_days,
    round(avg(o.promised_days), 2) as avg_promised_days,
    round(avg(o.is_late::int), 4) as late_rate,
    round(avg(o.review_score) filter (where not o.is_late), 2) as avg_review_on_time,
    round(avg(o.review_score) filter (where o.is_late), 2) as avg_review_late
from {{ ref('fct_orders') }} o
join {{ ref('dim_customers') }} c on o.customer_unique_id = c.customer_unique_id
where o.is_delivered
group by 1, 2

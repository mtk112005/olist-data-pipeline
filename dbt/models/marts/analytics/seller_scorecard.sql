-- Seller health: volume, revenue, speed of handing to carrier, lateness and reviews.
with items as (
    select * from {{ ref('fct_order_items') }}
),

orders as (
    select order_id, is_late, review_score, is_delivered, order_approved_at, order_delivered_carrier_date
    from {{ ref('fct_orders') }}
),

per_seller as (
    select
        i.seller_id,
        count(distinct i.order_id) as orders,
        sum(i.price)::numeric(14,2) as revenue,
        round(avg(i.distance_km), 0) as avg_distance_km,
        round(avg(i.seller_shipped_late::int), 4) as shipped_after_limit_rate,
        round(avg(extract(epoch from o.order_delivered_carrier_date - o.order_approved_at) / 86400.0), 2)
            as avg_days_to_carrier,
        round(avg(o.is_late::int) filter (where o.is_delivered), 4) as late_delivery_rate,
        round(avg(o.review_score), 2) as avg_review_score
    from items i
    join orders o on i.order_id = o.order_id
    group by 1
)

select
    p.*,
    s.state as seller_state,
    case
        when p.orders < 10 then 'new / low volume'
        when p.avg_review_score >= 4.3 and p.late_delivery_rate <= 0.05 then 'top'
        when p.avg_review_score < 3.5 or p.late_delivery_rate > 0.15 then 'at risk'
        else 'standard'
    end as seller_tier
from per_seller p
join {{ ref('dim_sellers') }} s on p.seller_id = s.seller_id

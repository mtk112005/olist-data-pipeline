-- RFM segmentation on real customers (customer_unique_id).
-- Note: ~97% of Olist customers buy once, so frequency barely separates them;
-- frequency is therefore bucketed (1 / 2 / 3+) instead of using ntile.
with ref_date as (
    select max(purchase_date) + 1 as as_of from {{ ref('fct_orders') }}
),

base as (
    select
        c.customer_unique_id,
        (r.as_of - c.last_order_date) as recency_days,
        c.order_count as frequency,
        c.lifetime_value as monetary
    from {{ ref('dim_customers') }} c
    cross join ref_date r
    where c.lifetime_value > 0
),

scored as (
    select
        *,
        ntile(5) over (order by recency_days desc) as recency_score,
        case when frequency >= 3 then 5 when frequency = 2 then 3 else 1 end as frequency_score,
        ntile(5) over (order by monetary) as monetary_score
    from base
)

select
    *,
    recency_score + frequency_score + monetary_score as rfm_total,
    case
        when frequency_score >= 3 and recency_score >= 4 then 'loyal - recent'
        when frequency_score >= 3 then 'loyal - lapsing'
        when recency_score >= 4 and monetary_score >= 4 then 'new high value'
        when recency_score >= 4 then 'new'
        when monetary_score >= 4 then 'lapsed high value'
        else 'lapsed'
    end as segment
from scored

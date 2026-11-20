select
    s.seller_id,
    s.zip_code_prefix,
    s.city,
    s.state,
    g.lat,
    g.lng,
    min(i.purchase_date) as first_sale_date,
    max(i.purchase_date) as last_sale_date
from {{ ref('stg_sellers') }} s
left join {{ ref('stg_geolocation') }} g on s.zip_code_prefix = g.zip_code_prefix
left join {{ ref('fct_order_items') }} i on s.seller_id = i.seller_id
group by 1, 2, 3, 4, 5, 6

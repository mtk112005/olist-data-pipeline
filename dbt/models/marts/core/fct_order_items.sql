{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
  )
}}

-- One row per item. Adds seller -> customer great-circle distance (haversine).
with items as (
    select * from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    where _loaded_at > (select coalesce(max(_loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    i.order_id,
    i.order_item_id,
    o.purchase_date,
    c.customer_unique_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    i.shipping_limit_date,
    o.order_delivered_carrier_date,
    coalesce(o.order_delivered_carrier_date > i.shipping_limit_date, false) as seller_shipped_late,
    s.state as seller_state,
    c.state as customer_state,
    round((2 * 6371 * asin(sqrt(
        power(sin(radians(gc.lat - gs.lat) / 2), 2)
        + cos(radians(gs.lat)) * cos(radians(gc.lat)) * power(sin(radians(gc.lng - gs.lng) / 2), 2)
    )))::numeric, 1) as distance_km,
    i._loaded_at
from items i
join {{ ref('stg_orders') }} o on i.order_id = o.order_id
left join {{ ref('stg_customers') }} c on o.customer_id = c.customer_id
left join {{ ref('stg_sellers') }} s on i.seller_id = s.seller_id
left join {{ ref('stg_geolocation') }} gs on s.zip_code_prefix = gs.zip_code_prefix
left join {{ ref('stg_geolocation') }} gc on c.zip_code_prefix = gc.zip_code_prefix

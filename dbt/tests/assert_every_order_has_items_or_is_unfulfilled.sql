-- Delivered orders must have at least one line item.
select order_id, order_status
from {{ ref('fct_orders') }}
where item_count = 0
  and order_status = 'delivered'

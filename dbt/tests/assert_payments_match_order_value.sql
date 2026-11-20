-- Reconciliation: what customers paid should equal items + freight.
-- Interest on instalments and vouchers create small legitimate gaps, so we only
-- flag gaps > R$5 and warn (not fail) unless more than 1% of orders break it.
{{ config(severity='error', error_if='>1000', warn_if='>0') }}

select order_id, order_value, payment_value, payment_value - order_value as gap
from {{ ref('fct_orders') }}
where order_status = 'delivered'
  and payment_value > 0
  and abs(payment_value - order_value) > 5

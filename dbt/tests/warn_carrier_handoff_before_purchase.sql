-- Known source issue: ~166 orders show the carrier hand-off BEFORE the purchase
-- (median ~36 min early, so mostly clock skew; worst case 171 days). Kept, flagged
-- in fct_orders.has_timeline_anomaly, and monitored here: warn always, fail if it grows.
{{ config(severity='error', error_if='>500', warn_if='>0') }}

select order_id, order_purchase_timestamp, order_delivered_carrier_date
from {{ ref('fct_orders') }}
where has_timeline_anomaly

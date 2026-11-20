-- customer_id is generated per ORDER in Olist; customer_unique_id is the person.
select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code_prefix,
    customer_city as city,
    customer_state as state
from {{ source('olist', 'customers') }}

{% snapshot snap_products %}

{{
  config(
    unique_key='product_id',
    strategy='check',
    check_cols=['category', 'weight_g', 'photos_qty', 'description_length'],
    invalidate_hard_deletes=True
  )
}}

-- SCD Type 2 history of the product catalogue: when a seller re-categorises a
-- product or edits its listing, the old version is closed (dbt_valid_to) and a
-- new one opened, so historical sales still join to the attributes they had.
select * from {{ ref('stg_products') }}

{% endsnapshot %}

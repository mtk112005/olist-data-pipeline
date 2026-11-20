select
    p.product_id,
    p.product_category_name as category_pt,
    coalesce(t.product_category_name_english, p.product_category_name, 'unknown') as category,
    p.product_name_lenght as name_length,          -- source column names are misspelt
    p.product_description_lenght as description_length,
    p.product_photos_qty as photos_qty,
    p.product_weight_g as weight_g,
    p.product_length_cm as length_cm,
    p.product_height_cm as height_cm,
    p.product_width_cm as width_cm,
    p.product_length_cm * p.product_height_cm * p.product_width_cm as volume_cm3
from {{ source('olist', 'products') }} p
left join {{ source('olist', 'category_translation') }} t
    on p.product_category_name = t.product_category_name

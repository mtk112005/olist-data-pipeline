select
    product_id,
    category,
    category_pt,
    name_length,
    description_length,
    photos_qty,
    weight_g,
    volume_cm3,
    case
        when weight_g is null then 'unknown'
        when weight_g < 500 then 'light (<0.5kg)'
        when weight_g < 2000 then 'medium (0.5-2kg)'
        when weight_g < 10000 then 'heavy (2-10kg)'
        else 'bulky (10kg+)'
    end as weight_band
from {{ ref('stg_products') }}

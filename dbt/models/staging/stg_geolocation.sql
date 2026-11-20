-- ~1M raw rows -> one point per zip prefix. Points outside Brazil's bounding box are bad geocodes.
select
    geolocation_zip_code_prefix as zip_code_prefix,
    avg(geolocation_lat) as lat,
    avg(geolocation_lng) as lng,
    count(*) as raw_points
from {{ source('olist', 'geolocation') }}
where geolocation_lat between -34 and 6
  and geolocation_lng between -74 and -34
group by 1

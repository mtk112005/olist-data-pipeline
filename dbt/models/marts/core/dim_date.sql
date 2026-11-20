select
    d::date as date_day,
    extract(year from d)::int as year,
    extract(quarter from d)::int as quarter,
    extract(month from d)::int as month,
    to_char(d, 'YYYY-MM') as year_month,
    extract(isodow from d)::int as day_of_week,
    to_char(d, 'Dy') as day_name,
    extract(isodow from d) in (6, 7) as is_weekend,
    -- Black Friday = 4th Friday of November (the biggest spike in Olist, 2017-11-24)
    (extract(month from d) = 11 and extract(isodow from d) = 5
        and extract(day from d) between 22 and 28) as is_black_friday
from generate_series('2016-01-01'::date, '2018-12-31'::date, interval '1 day') d

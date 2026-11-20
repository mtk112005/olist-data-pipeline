-- A few orders carry more than one review; keep the most recent answer per order.
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by review_answer_timestamp desc nulls last, review_creation_date desc
        ) as rn
    from {{ source('olist', 'order_reviews') }}
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_comment_message is not null as has_comment,
    review_creation_date,
    review_answer_timestamp,
    _loaded_at
from ranked
where rn = 1

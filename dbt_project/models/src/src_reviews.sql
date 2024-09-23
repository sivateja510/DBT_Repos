with raw_reviews as(
    select * from {{ source('airbnb','reviews') }}
)
select
listing_id,
date as review_date,
REVIEWER_NAME,
comments as review_text,
sentiment as review_sentiment
from raw_reviews
with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as (
    select 
        
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::Object:"ASIN"::STRING as ASIN,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"SalesRankings"::ARRAY as summary
    from source_data
),
flatten_notification as (
    select
    ASIN,
    offer.value:"ProductCategoryId"::STRING as ProductCategoryId,
    offer.value:"Rank"::BIGINT as Rank,

    from flatten_payload,
    lateral flatten(input => flatten_payload.summary) as offer,
)
select * from flatten_notification



with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as(
    select 
        raw_data:"NotificationMetadata"::Object:"PublishTime"::STRING as PublishTime,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::Object:"ASIN"::STRING as ASIN ,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::Object:"ItemCondition"::STRING as ItemCondition 
    from source_data
)
select * from flatten_payload
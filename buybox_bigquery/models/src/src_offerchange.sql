with source_data as (
    select parse_json(data.message_body) as raw_data,
            parse_json(data.message_body).Payload.AnyOfferChangedNotification.OfferChangeTrigger as offer
     from {{source("dbt-cloud-01","BB_Raw_data")}} as data
),
flatten_payload as(
    select 
    GENERATE_UUID() as SurrogateKey,
    JSON_VALUE(raw_data.NotificationMetadata.PublishTime) as PublishTime,  
    JSON_VALUE(offer.ASIN) as ASIN,
    JSON_VALUE(offer.ItemCondition) as ItemCondition,
    from source_data
)
select * from flatten_payload
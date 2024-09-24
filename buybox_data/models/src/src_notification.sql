with source_data as(
    select parse_json(message_body) as raw_data,
    raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::ARRAY as offer 
    from {{source("buybox","BB_Raw_data")}}
),
flatten_data as(
    select 
        raw_data:"NotificationMetadata"::Object:"NotificationId" :: STRING as NotificationId,
        raw_data:"NotificationMetadata"::Object:"PublishTime" :: TIMESTAMP as PublishTime,
        raw_data:"NotificationMetadata"::Object:"SubscriptionId" :: STRING as SubscriptionId,
        raw_data:"NotificationMetadata"::Object:"ApplicationId" :: STRING as ApplicationId,
        raw_data:"NotificationType"::STRING as NotificationType,
        raw_data:"NotificationVersion"::FLOAT as NotificationVersion,
        offer.value :"ASIN"::STRING as ASIN
        from source_data,
         LATERAL FLATTEN(input=>source_data.offer) as offer
)
select * from flatten_data

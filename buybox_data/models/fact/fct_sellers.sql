{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        merge_update_columns=['IsFeaturedMerchant', 'IsFulfilledByAmazon']
    )
}}
with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as(
    select 
    raw_data:"EventTime"::timestamp as EventTime,
    raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY as offers from source_data
)
select 
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as id,
    EventTime as EventTime,
    offer.value:"SellerId"::STRING as SellerId,
    offer.value:"IsFeaturedMerchant"::BOOLEAN as IsFeaturedMerchant,
    offer.value:"IsFulfilledByAmazon"::BOOLEAN as IsFulfilledByAmazon,
    md5(concat(offer.value:"SellerId", EventTime,IsFeaturedMerchant,IsFulfilledByAmazon,id)) as surrogate_key,
    current_timestamp() as updated_at
from flatten_payload f, LATERAL FLATTEN(input=>f.offers) as offer


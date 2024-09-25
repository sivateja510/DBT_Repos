{{
    config(
        materialized='incremental',
        unique_key='SellerId',
        merge_update_columns=['IsFeaturedMerchant','IsFulfilledByAmazon']
    )
}}
with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as(
    select raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY as offers from source_data
)
select 
    offer.value:"SellerId"::STRING as SellerId,
    offer.value:"IsFeaturedMerchant"::BOOLEAN as IsFeaturedMerchant,
    offer.value:"IsFulfilledByAmazon"::BOOLEAN as IsFulfilledByAmazon
from flatten_payload f, LATERAL FLATTEN(input=>f.offers) as offer


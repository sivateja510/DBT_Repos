with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as(
    select raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::ARRAY as offer from source_data
)
select offer.value:"ASIN"::STRING as ASIN,
       offer.value:"MarketplaceId"::STRING as MarketplaceId
from flatten_payload f, LATERAL FLATTEN(input=>f.offer) as offer

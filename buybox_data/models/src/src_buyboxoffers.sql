with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as (
    select 
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"NumberOfBuyBoxEligibleOffers"::ARRAY as summary,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"NumberOfOffers"::ARRAY as offers
    from source_data
),
flatten_notification as (
    select
        offer.value:"Condition"::STRING as Condition,
        offer.value:"FulfillmentChannel"::STRING as EligibleOfferFullfillmentChannel,
        offer.value:"OfferCount"::INTEGER as EligibleOfferCount,
        offers.value:"Condition"::STRING as NumberOfferCondition,
        offers.value:"FulfillmentChannel"::STRING as NumberOfferFullfillmentChannel,
        offers.value:"OfferCount"::INTEGER as NumberOfferCount,
        
    from flatten_payload,
    lateral flatten(input => flatten_payload.summary) as offer,
    lateral flatten(input => flatten_payload.offers) as offers,
)
select * from flatten_notification


with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as (
    select 
        raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING as NotificationId,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"LowestPrices"::ARRAY as Offers
    from source_data
),
flatten_notification as (
    select
        NotificationId,
        offer.value:"Condition"::STRING as Condition,
        offer.value:"FulfillmentChannel"::STRING as FulfillmentChannel,
        offer.value:"LandedPrice"::Object:"Amount"::FLOAT as LandedPriceAmount,
        offer.value:"LandedPrice"::Object:"CurrencyCode"::STRING as LandedPriceCurrencyCode,
        offer.value:"ListingPrice"::Object:"Amount"::FLOAT as ListingPriceAmount,
        offer.value:"Shipping"::Object:"Amount"::FLOAT as ShippingPrice
    from flatten_payload,
    lateral flatten(input => flatten_payload.Offers) as offer
)
select * from flatten_notification
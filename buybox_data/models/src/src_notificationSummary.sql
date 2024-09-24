with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as (
    select 
        raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING as NotificationId,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::Object:"BuyBoxPrices"::ARRAY as Offers,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Summary"::ARRAY as summary
    from source_data
),
flatten_notification as (
    select
        NotificationId,
        offer.value:"LandedPrice"::Object:"Amount"::FLOAT as BB_LandingPrice,
        offer.value:"ListingPrice"::Object:"Amount"::FLOAT as BB_ListingPrice,
        offer.value:"Shipping"::Object:"Amount"::FLOAT as BB_ShippingCost,
        offer.value:"Shipping"::Object:"CurrencyCode"::STRING as BB_ShippingCurrencyCode,
        summary.value:"ListPrice"::Object:"Amount"::FLOAT as ListPriceAmount
    from flatten_payload,
    lateral flatten(input => flatten_payload.Offers) as offer,
    lateral flatten(input => flatten_payload.summary) as summary
)
select * from flatten_notification

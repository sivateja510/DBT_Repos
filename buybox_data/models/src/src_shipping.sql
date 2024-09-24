
with source_data as(
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as (
    select 
        raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING as NotificationId,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY as Offers
    from source_data
),
flatten_shipping as (
    select 
        NotificationId,
        offer.value:"Shipping"::Object:"Amount"::FLOAT as ShippingAmount,
        offer.value:"Shipping"::Object:"CurrencyCode"::STRING as CurrencyCode,
        offer.value:"ShippingTime"::Object:"AvailabilityType"::STRING as AvailabilityType,
        offer.value:"ShippingTime"::Object:"AvailableDate"::STRING as AvailableDate,
        offer.value:"ShippingTime"::Object:"MaximumHours"::STRING as MaximumHours,
        offer.value:"ShippingTime"::Object:"MinimumHours"::STRING as MinimumHours,
        offer.value:"ShipsDomestically"::STRING as ShipsDomestically
    from flatten_payload,
    lateral flatten(input => flatten_payload.Offers) as offer
)
select * from flatten_shipping
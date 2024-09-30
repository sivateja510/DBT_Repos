with source_data as (
    select parse_json(message_body) as raw_data
    from {{source("buybox","BB_Raw_data")}}
),
flatten_payload as (
    select 
        raw_data:"EventTime"::TIMESTAMP as EventTime,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY as offer,
        raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING as NotificationId
    from source_data
)
select 
    EventTime,
    NotificationId,
    offer.value:"IsFeaturedMerchant"::BOOLEAN as IsFeaturedMerchant,
    offer.value:"IsFulfilledByAmazon"::BOOLEAN as IsFulfilledByAmazon,
    offer.value:"IsFulfilledByAmazon"::BOOLEAN as IsBuyBoxWinner,
    offer.value:"ListingPrice"::Object:"Amount"::FLOAT as ListingPrice,
    offer.value:"PrimeInformation"::Object:"IsOfferNationalPrime"::BOOLEAN as IsOfferNationalPrime,
    offer.value:"PrimeInformation"::Object:"IsOfferPrime"::BOOLEAN as IsOfferPrime,
    offer.value:"SellerFeedbackRating"::Object:"SellerPositiveFeedbackRating"::INTEGER as SellerPositiveFeedbackRating,
    offer.value:"SellerFeedbackRating"::Object:"SellerFeedbackCount"::BIGINT as SellerFeedbackCount,
    offer.value:"ShippingTime"::Object:"AvailabilityType"::STRING as AvailabilityType,
    offer.value:"ShippingTime"::Object:"MaximumHours"::INTEGER as MaximumHours,
    offer.value:"ShippingTime"::Object:"MinimumHours"::INTEGER as MinimumHours,
    offer.value:"Shipping"::Object:"Amount"::FLOAT as ShippingCost,
    offer.value:"SellerId"::STRING as SellerId,
    offer.value:"ShipsDomestically"::BOOLEAN as ShipsDomestically
from flatten_payload, lateral flatten(input => flatten_payload.offer) as offer

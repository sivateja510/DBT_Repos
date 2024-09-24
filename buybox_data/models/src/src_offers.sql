

with source_data as (
    select 
        parse_json(message_body) as raw_data,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::ARRAY as offer 
    from {{ source("buybox","BB_Raw_data")}}
),
flatten_payload as (
    select 
        raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING as NotificationId,
        offer.value:"ASIN"::STRING as ASIN,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY as Offers
    from source_data, 
    lateral flatten(input => source_data.offer) as offer
),
flatten_offer as (
    select 
        NotificationId,
        ASIN,
        offer.value:"SellerId"::STRING as SELLERID,
        offer.value:"IsBuyBoxWinner"::BOOLEAN as IsBuyBoxWinner,
        offer.value:"ListingPrice"::Object:"Amount"::FLOAT as ListingPriceAmount,
        offer.value:"ListingPrice"::Object:"CurrencyCode"::STRING as ListingPriceCurrencyCode,
        offer.value:"PrimeInformation"::Object:"IsOfferNationalPrime"::BOOLEAN as PrimeInformation_IsOfferNationalPrime,
        offer.value:"PrimeInformation"::Object:"IsOfferPrime"::BOOLEAN as PrimeInformation_IsOfferPrime,
        offer.value:"SubCondition"::STRING as SubCondition 
    from flatten_payload, 
    lateral flatten(input => flatten_payload.Offers) as offer
)
select 
    NotificationId,
    ASIN,
    SELLERID,
    IsBuyBoxWinner,
    ListingPriceAmount,
    PrimeInformation_IsOfferNationalPrime,
    PrimeInformation_IsOfferPrime
from flatten_offer
WITH source_data AS (
    SELECT 
        PARSE_JSON(message_body) AS raw_data,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::ARRAY AS offer 
    FROM {{ source("buybox", "BB_Raw_data") }}
),
flatten_payload AS (
    SELECT 
        raw_data:"NotificationMetadata"::Object:"PublishTime"::STRING AS PublishTime,
        offer.value:"ASIN"::STRING AS ASIN,
        raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY AS Offers
    FROM source_data, 
    LATERAL FLATTEN(input => source_data.offer) AS offer
),
flatten_offer AS (
    SELECT 
        PublishTime,
        offer.value:"IsBuyBoxWinner"::Boolean AS IsBuyBoxWinner,
        offer.value:"SellerId"::STRING AS SellerId,
        offer.value:"ListingPrice"::Object:"Amount"::FLOAT AS ListingPrice,
        ASIN
    FROM flatten_payload, 
    LATERAL FLATTEN(input => flatten_payload.Offers) AS offer
),
buybox_winners AS (
    SELECT 
        PublishTime,
        ASIN,
        ListingPrice AS BB_ListingPrice
    FROM flatten_offer
    WHERE IsBuyBoxWinner = TRUE
)

SELECT 
    fo.PublishTime,
    fo.ASIN,
    fo.SellerId,
    fo.ListingPrice AS LISTINGPRICEAMOUNT,
    bw.BB_ListingPrice,
    fo.IsBuyBoxWinner,
    (fo.ListingPrice - bw.BB_ListingPrice) AS ListingPriceDifference
FROM flatten_offer AS fo
JOIN buybox_winners AS bw
    ON fo.PublishTime = bw.PublishTime AND fo.ASIN = bw.ASIN
ORDER BY fo.PublishTime, fo.ASIN ,fo.SellerId, fo.ListingPrice
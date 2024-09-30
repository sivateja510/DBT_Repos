WITH source_data AS (
    SELECT message_id, PARSE_JSON(MESSAGE_BODY) AS raw_data
    FROM {{source("buybox","BB_Raw_data")}}
),
flatten_payload AS (
    SELECT
        message_id,
        raw_data:"EventTime"::STRING AS EventTime,
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::OBJECT:"ASIN"::STRING AS ASIN,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY AS offers
    FROM source_data
),
flatten_offers AS (
    SELECT 
        message_id,
        NotificationId,
        EventTime, 
        ASIN, 
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY NotificationId DESC) AS OfferId,
        offers.value:"SellerId"::STRING AS SellerId,
        offers.value:"IsBuyBoxWinner"::STRING AS IsBuyBoxWinner,
        offers.value:"IsFeaturedMerchant"::STRING AS IsFeaturedMerchant,
        offers.value:"ListingPrice"::OBJECT:"Amount"::FLOAT AS ListingPriceAmount,
        offers.value:"ListingPrice"::OBJECT:"CurrencyCode"::STRING AS ListingPriceCurrencyCode,
        offers.value:"PrimeInformation"::OBJECT:"IsOfferNationalPrime"::BOOLEAN AS IsOfferNationalPrime,
        offers.value:"PrimeInformation"::OBJECT:"IsOfferPrime"::BOOLEAN AS PrimeInformation,
        offers.value:"IsFulfilledByAmazon"::STRING AS IsFulfilledByAmazon,
        offers.value:"SellerFeedbackRating"::OBJECT:"FeedbackCount"::STRING AS FeedbackCount,
        offers.value:"SellerFeedbackRating"::OBJECT:"SellerPositiveFeedbackRating"::STRING AS SellerPositiveFeedbackRating
    FROM flatten_payload,
    LATERAL FLATTEN(input => flatten_payload.offers) AS offers
),
ranked_offers AS (
    SELECT
        OfferId,
        EventTime,
        ASIN,
        message_id,
        NotificationId,
        SellerId,
        IsBuyBoxWinner,
        IsFulfilledByAmazon,
        IsFeaturedMerchant,
        SellerPositiveFeedbackRating,
        ListingPriceAmount,
        ListingPriceCurrencyCode,
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY 
            IsBuyBoxWinner DESC, 
            IsFulfilledByAmazon DESC, 
            IsFeaturedMerchant ASC, 
            SellerPositiveFeedbackRating DESC,
            ListingPriceAmount ASC) AS ranked
    FROM flatten_offers
)
SELECT 
    ranked,
    OfferId,
    EventTime,
    ASIN,
    message_id,
    NotificationId,
    SellerId,
    IsBuyBoxWinner,
    IsFulfilledByAmazon,
    IsFeaturedMerchant,
    SellerPositiveFeedbackRating,
    ListingPriceAmount,
    ListingPriceCurrencyCode
FROM ranked_offers 
ORDER BY 
    EventTime ASC,
    IsBuyBoxWinner DESC,
    IsFulfilledByAmazon DESC,
    IsFeaturedMerchant ASC,
    SellerPositiveFeedbackRating DESC,
    ListingPriceAmount ASC
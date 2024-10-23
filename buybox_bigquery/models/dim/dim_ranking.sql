WITH source_data AS (
    SELECT 
        JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification') AS raw_data,
        PARSE_JSON(data.message_body) as parse_data,data.message_id
    FROM {{source("dbt-cloud-01","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT
        message_id, 
        JSON_VALUE(parse_data.EventTime) AS EventTime,
        JSON_VALUE(parse_data.NotificationMetadata.NotificationId) AS NotificationId,
        JSON_VALUE(parse_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN) AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.Offers') AS offers ,
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() as SurrogateKey,
        message_id,
        ASIN,
        EventTime,
        NotificationId,
        JSON_EXTRACT(offer, '$.SellerId') AS SellerId,
        JSON_EXTRACT(offer, '$.IsBuyBoxWinner') AS IsBuyBoxWinner,
        JSON_EXTRACT(offer, '$.IsFeaturedMerchant') AS IsFeaturedMerchant,
        JSON_EXTRACT(offer, '$.ListingPrice.Amount') AS ListingPriceAmount,
        JSON_EXTRACT(offer, '$.ListingPrice.CurrencyCode') AS ListingPriceCurrencyCode,
        JSON_EXTRACT(offer, '$.PrimeInformation.IsOfferNationalPrime') AS IsOfferNationalPrime,
        JSON_EXTRACT(offer, '$.PrimeInformation.IsOfferPrime') AS IsOfferPrime,
        JSON_EXTRACT(offer, '$.IsFulfilledByAmazon') AS IsFulfilledByAmazon,
        JSON_EXTRACT(offer, '$.SellerFeedbackRating.FeedbackCount') AS FeedbackCount,
        JSON_EXTRACT(offer, '$.SellerFeedbackRating.SellerPositiveFeedbackRating') AS SellerPositiveFeedbackRating,
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY NotificationId DESC) AS OfferId,
        
    FROM flatten_payload, UNNEST(offers) AS offer 
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

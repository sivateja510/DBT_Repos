WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data
    FROM {{source("dbt-cloud-01","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT 
        JSON_VALUE(parse_data.NotificationMetadata.NotificationId) AS NotificationId,  
        JSON_VALUE(raw_data, '$.OfferChangeTrigger.ASIN') AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.Offers') AS offers
    FROM source_data
),
flatten_offers AS (
    SELECT
        GENERATE_UUID() as SurrogateKey,
        NotificationId, 
        ASIN,
        JSON_EXTRACT(offer, '$.SellerId') AS SellerId,
        JSON_EXTRACT(offer, '$.IsBuyBoxWinner') AS IsBuyBoxWinner,
        JSON_EXTRACT(offer, '$.ListingPrice.Amount') AS Amount,
        JSON_EXTRACT(offer, '$.ListingPrice.CurrencyCode') AS CurrencyCode,
        JSON_EXTRACT(offer, '$.PrimeInformation.IsOfferNationalPrime') AS IsOfferNationalPrime,
        JSON_EXTRACT(offer, '$.PrimeInformation.IsOfferPrime') AS IsOfferPrime,
        JSON_EXTRACT(offer, '$.SubCondition') AS SubCondition

    FROM flatten_payload, UNNEST(offers) AS offer 
)
SELECT * FROM flatten_offers


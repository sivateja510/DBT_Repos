WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data
    FROM {{source("dbt-cloud-01","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT
        parse_data.NotificationMetadata.NotificationId as NotificationId,
        JSON_EXTRACT_ARRAY(raw_data, '$.LowestPrices') AS offers,
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() as SurrogateKey,
        NotificationId,
        JSON_EXTRACT(offers, '$.Condition') AS Condition,
        JSON_EXTRACT(offers, '$.FulfillmentChannel') AS FulfillmentChannel,
        JSON_EXTRACT(offers, '$.LandedPrice.Amount') AS Amount,
        JSON_EXTRACT(offers, '$.LandedPrice.CurrencyCode') AS CurrencyCode,
        JSON_EXTRACT(offers, '$.ListingPrice.Amount') AS Listing_Amount,
        JSON_EXTRACT(offers, '$.Shipping.Amount') AS Shipping_Amount,

    FROM flatten_payload, UNNEST(offers) AS offers 
)
SELECT * FROM flatten_offers


WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data
    FROM {{source("dbt-cloud-01","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT 
        JSON_VALUE(parse_data.NotificationMetadata.NotificationId) AS NotificationId,
        JSON_EXTRACT_ARRAY(raw_data, '$.BuyBoxPrices') AS offers,
        JSON_EXTRACT(raw_data, '$.ListPrice.Amount') AS ListPrice
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() as SurrogateKey,
        NotificationId,
        JSON_EXTRACT(offer, '$.LandedPrice.Amount') AS BB_LandingPrice,
        JSON_EXTRACT(offer, '$.ListingPrice.Amount') AS BB_ListingPrice,
        JSON_EXTRACT(offer, '$.Shipping.Amount') AS BB_LShippingPrice,
        JSON_EXTRACT(offer, '$.Shipping.CurrencyCode') AS BB_ShippingCurrencyCode,
        ListPrice
    FROM flatten_payload, UNNEST(offers) AS offer 
)
SELECT * FROM flatten_offers


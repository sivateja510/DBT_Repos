WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data
    FROM {{source("dbt-cloud-01","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT 
        JSON_VALUE(parse_data.Payload.AnyOfferChangedNotification.OfferChangeTrigger.ASIN) AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.SalesRankings') AS SalesRankings
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() as SurrogateKey,
        ASIN,
        JSON_EXTRACT(offer, '$.ProductCategoryId') AS ProductCategoryId,
        JSON_EXTRACT(offer, '$.Rank') AS Rank

    FROM flatten_payload, UNNEST(SalesRankings) AS offer 
)
SELECT * FROM flatten_offers

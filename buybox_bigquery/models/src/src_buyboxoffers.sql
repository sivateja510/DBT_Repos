WITH source_data AS (
    SELECT 
      JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification.Summary') AS raw_data,
      PARSE_JSON(data.message_body) as parse_data
    FROM {{source("dbt-cloud-01","BB_Raw_data")}} as data
),
flatten_payload AS (
    SELECT 
        JSON_EXTRACT_ARRAY(raw_data, '$.NumberOfBuyBoxEligibleOffers') AS offers,
        JSON_EXTRACT_ARRAY(raw_data, '$.NumberOfOffers') AS offer
    FROM source_data
),
flatten_offers AS (
    SELECT 
        GENERATE_UUID() as SurrogateKey,
        JSON_EXTRACT(offers, '$.Condition') AS Condition,
        JSON_EXTRACT(offers, '$.FulfillmentChannel') AS FulfillmentChannel,
        JSON_EXTRACT(offers, '$.OfferCount') AS OfferCount,
        JSON_EXTRACT(offer, '$.Condition') AS NumberOfOffers_Condition,
        JSON_EXTRACT(offer, '$.FulfillmentChannel') AS NumberOfOffers_FulfillmentChannel,
        JSON_EXTRACT(offer, '$.OfferCount') AS NumberOfOffers_OfferCount

    FROM flatten_payload, UNNEST(offers) AS offers,UNNEST(offer) as offer 
)
SELECT * FROM flatten_offers


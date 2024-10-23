{{
    config(
        materialized='incremental',
        unique_key='SurrogateKey',
        merge_update_columns=['IsFeaturedMerchant', 'IsFulfilledByAmazon']
    )
}}
WITH source_data AS (
    SELECT 
        JSON_EXTRACT(data.message_body, '$.Payload.AnyOfferChangedNotification') AS raw_data,
        PARSE_JSON(data.message_body) as parse_data,data.message_id
    FROM {{source("dbt-cloud-01","BB_Raw_data")}} as data
),flatten_payload AS (
    SELECT
        JSON_VALUE(parse_data.EventTime) AS EventTime,
        JSON_EXTRACT_SCALAR(raw_data, '$.ASIN') AS ASIN,
        JSON_EXTRACT_ARRAY(raw_data, '$.Offers') AS offers
    FROM source_data
),
flatten_offers AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as id, 
        EventTime,
        JSON_EXTRACT(offer, '$.SellerId') AS SellerId,
        JSON_EXTRACT(offer, '$.IsFeaturedMerchant') AS IsFeaturedMerchant,
        JSON_EXTRACT(offer, '$.IsFulfilledByAmazon') AS IsFulfilledByAmazon,
        ROW_NUMBER() OVER (PARTITION BY ASIN ORDER BY EventTime DESC) AS OfferId,
        GENERATE_UUID() as SurrogateKey,
        current_timestamp() as updated_at
    FROM flatten_payload, UNNEST(offers) AS offer 
) 
SELECT * FROM flatten_offers

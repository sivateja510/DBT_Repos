{% macro flatten_payload(source_table) %}
    WITH source_data AS (
        SELECT 
            PARSE_JSON(message_body) AS raw_data,
            raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::ARRAY AS offer 
        FROM {{  source("buybox", "BB_Raw_data")  }}
    ),
    flatten_payload AS (
        SELECT 
            raw_data:"NotificationMetadata"::Object:"PublishTime"::STRING AS PublishTime,
            offer.value:"ASIN"::STRING AS ASIN,
            raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY AS Offers
        FROM source_data, 
        LATERAL FLATTEN(input => source_data.offer) AS offer
    )
    SELECT * FROM flatten_payload;
{% endmacro %}

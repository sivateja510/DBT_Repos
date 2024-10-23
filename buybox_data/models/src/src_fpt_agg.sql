-- WITH source_data AS (
--     SELECT 
--         PARSE_JSON(message_body) AS raw_data,
--         raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"OfferChangeTrigger"::ARRAY AS offer 
--     FROM {{ source("buybox", "BB_Raw_data") }}
-- ),
-- flatten_payload AS (
--     SELECT 
--         -- raw_data:"NotificationMetadata"::Object:"NotificationId"::STRING AS NotificationId,
--         raw_data:"NotificationMetadata"::Object:"PublishTime"::STRING as PublishTime,
--         offer.value:"ASIN"::STRING AS ASIN,
--         raw_data:"Payload"::Object:"AnyOfferChangedNotification"::Object:"Offers"::ARRAY AS Offers
--     FROM source_data, 
--     LATERAL FLATTEN(input => source_data.offer) AS offer
-- ),
-- flatten_offer AS (
--     SELECT 
--         PublishTime,
--         offer.value:"SellerId"::STRING AS SellerId,
--         offer.value:"SellerFeedbackRating"::Object:"FeedbackCount"::BIGINT AS FeedbackCount,
--         offer.value:"SellerFeedbackRating"::Object:"SellerPositiveFeedbackRating"::BIGINT AS SellerPositiveFeedbackRating,
--         offer.value:"ShippingTime"::Object:"MaximumHours"::FLOAT AS MaximumHours,
--         offer.value:"ShippingTime"::Object:"MinimumHours"::BIGINT AS MinimumHours,
--         ASIN
--     FROM flatten_payload, 
--     LATERAL FLATTEN(input => flatten_payload.Offers) AS offer
-- ),
-- feedback_stats AS (
--     SELECT
--         PublishTime,
--         AVG(FeedbackCount) AS mean_feedback_count,
--         MAX(FeedbackCount) AS highest_feedback_count,
--         AVG(SellerPositiveFeedbackRating) AS mean_feedback_rate,
--         MAX(SellerPositiveFeedbackRating) AS highest_feedback_rate,
--         AVG(MaximumHours) AS mean_max_hours,
--         MIN(MaximumHours) AS min_max_hours,
--         AVG(MinimumHours) AS mean_min_hours,
--         MIN(MinimumHours) AS min_min_hours
--     FROM flatten_offer
--     GROUP BY PublishTime
-- )
-- SELECT
--     fo.PublishTime,
--     fo.ASIN,
--     fo.FeedbackCount,
--     fo.SellerPositiveFeedbackRating,
--     ABS(fo.FeedbackCount - fs.mean_feedback_count) AS diff_fpt_cnt_mean,
--     ABS(fo.FeedbackCount - fs.highest_feedback_count) AS diff_fpt_cnt_highest,
--     ABS(fo.SellerPositiveFeedbackRating - fs.mean_feedback_rate) AS diff_fpt_rate_mean,
--     fo.SellerPositiveFeedbackRating - fs.highest_feedback_rate AS diff_fpt_rate_highest,

--     -- Time Metrics
--     fo.MaximumHours,
--     fo.MinimumHours,
--     fs.mean_max_hours,
--     fs.mean_min_hours,
--     ABS(fo.MaximumHours - fs.mean_max_hours) AS diff_max_hours_mean,
--     ABS(fo.MinimumHours - fs.mean_min_hours) AS diff_min_hours_mean,
--     ABS(fs.mean_max_hours - fs.min_max_hours) AS max_hours_mean_diff,
--     ABS(fs.mean_min_hours - fs.min_min_hours) AS min_hours_mean_diff
-- FROM 
--     flatten_offer fo
-- JOIN 
--     feedback_stats fs
-- ON 
--     fo.PublishTime = fs.PublishTime


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
        offer.value:"SellerId"::STRING AS SellerId,
        offer.value:"ListingPrice"::Object:"Amount"::FLOAT AS ListingPrice,  -- Added ListingPrice
        offer.value:"SellerFeedbackRating"::Object:"FeedbackCount"::BIGINT AS FeedbackCount,
        offer.value:"SellerFeedbackRating"::Object:"SellerPositiveFeedbackRating"::BIGINT AS SellerPositiveFeedbackRating,
        offer.value:"ShippingTime"::Object:"MaximumHours"::FLOAT AS MaximumHours,
        offer.value:"ShippingTime"::Object:"MinimumHours"::BIGINT AS MinimumHours,
        ASIN
    FROM flatten_payload, 
    LATERAL FLATTEN(input => flatten_payload.Offers) AS offer
),
feedback_stats AS (
    SELECT
        PublishTime,
        AVG(FeedbackCount) AS mean_feedback_count,
        MAX(FeedbackCount) AS highest_feedback_count,
        AVG(SellerPositiveFeedbackRating) AS mean_feedback_rate,
        MAX(SellerPositiveFeedbackRating) AS highest_feedback_rate,
        AVG(MaximumHours) AS mean_max_hours,
        MIN(MaximumHours) AS min_max_hours,
        AVG(MinimumHours) AS mean_min_hours,
        MIN(MinimumHours) AS min_min_hours
    FROM flatten_offer
    GROUP BY PublishTime
)
SELECT
    fo.PublishTime,
    fo.ASIN,
    fo.SellerId,  -- Include SellerId directly from flatten_offer
    fo.FeedbackCount,
    fo.SellerPositiveFeedbackRating,
    ABS(fo.FeedbackCount - fs.mean_feedback_count) AS diff_fpt_cnt_mean,
    ABS(fo.FeedbackCount - fs.highest_feedback_count) AS diff_fpt_cnt_highest,
    ABS(fo.SellerPositiveFeedbackRating - fs.mean_feedback_rate) AS diff_fpt_rate_mean,
    fo.SellerPositiveFeedbackRating - fs.highest_feedback_rate AS diff_fpt_rate_highest,

    -- Time Metrics
    fo.MaximumHours,
    fo.MinimumHours,
    fs.mean_max_hours,
    fs.mean_min_hours,
    ABS(fo.MaximumHours - fs.mean_max_hours) AS diff_max_hours_mean,
    ABS(fo.MinimumHours - fs.mean_min_hours) AS diff_min_hours_mean,
    ABS(fs.mean_max_hours - fs.min_max_hours) AS max_hours_mean_diff,
    ABS(fs.mean_min_hours - fs.min_min_hours) AS min_hours_mean_diff,

    -- Listing Price
    fo.ListingPrice as  -- Added ListingPrice at the end
FROM 
    flatten_offer fo
JOIN 
    feedback_stats fs
ON 
    fo.PublishTime = fs.PublishTime
ORDER BY 
    PublishTime, ASIN, SellerId,LISTINGPRICEAMOUNT

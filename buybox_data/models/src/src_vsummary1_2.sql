WITH source_data AS (
    SELECT 
        PublishTime, 
        ASIN,
        SellerId,
        ListingPriceAmount 
    FROM {{ ref('src_offers') }}
),
price_data AS (
    SELECT 
        PublishTime, 
        ASIN,
        SellerId,
        AVG(ListingPriceAmount) AS mean_listing_price,
        MIN(ListingPriceAmount) AS lowest_listing_price
    FROM source_data
    GROUP BY 
        PublishTime, 
        ASIN, 
        SellerId 
)
SELECT 
    sd.PublishTime,
    sd.ASIN,
    sd.SellerId,
    sd.ListingPriceAmount,
    pd.mean_listing_price,
    ABS(sd.ListingPriceAmount - pd.mean_listing_price) AS diff_listing_price_mean,
    sd.ListingPriceAmount - pd.lowest_listing_price AS diff_listing_price_lowest,
    CASE 
        WHEN sd.ListingPriceAmount = pd.lowest_listing_price THEN 1
        ELSE 0
    END AS is_lowest
FROM 
    source_data sd
JOIN 
    price_data pd
ON 
    sd.PublishTime = pd.PublishTime 
    AND sd.ASIN = pd.ASIN 
    AND sd.SellerId = pd.SellerId
order by PublishTime,ASIN,SELLERID,ListingPriceAmount



-- WITH source_data AS (
--     SELECT 
--         PublishTime, 
--         ASIN,
--         ListingPriceAmount 
--     FROM {{ ref('src_offers') }}
-- ),
-- price_data AS (
--     SELECT  
--         ASIN,
--         PublishTime,
--         AVG(ListingPriceAmount) AS mean_listing_price,
--         MIN(ListingPriceAmount) AS lowest_listing_price
--     FROM source_data
--     GROUP BY 
--         PublishTime, 
--         ASIN
-- )
-- SELECT 
--     sd.PublishTime,
--     sd.ASIN,
--     sd.ListingPriceAmount,
--     (SELECT AVG(ListingPriceAmount) 
--      FROM source_data pd 
--      WHERE pd.PublishTime = sd.PublishTime AND pd.ASIN = sd.ASIN) AS mean_listing_price,
--     (SELECT MIN(ListingPriceAmount) 
--      FROM source_data pd 
--      WHERE pd.PublishTime = sd.PublishTime AND pd.ASIN = sd.ASIN) AS lowest_listing_price,
--     ABS(sd.ListingPriceAmount - 
--         (SELECT AVG(ListingPriceAmount) 
--          FROM source_data pd 
--          WHERE pd.PublishTime = sd.PublishTime AND pd.ASIN = sd.ASIN)) AS diff_listing_price_mean,
--     sd.ListingPriceAmount - 
--         (SELECT MIN(ListingPriceAmount) 
--          FROM source_data pd 
--          WHERE pd.PublishTime = sd.PublishTime AND pd.ASIN = sd.ASIN) AS diff_listing_price_lowest,
--     CASE 
--         WHEN sd.ListingPriceAmount = 
--             (SELECT MIN(ListingPriceAmount) 
--              FROM source_data pd 
--              WHERE pd.PublishTime = sd.PublishTime AND pd.ASIN = sd.ASIN) 
--         THEN 1
--         ELSE 0
--     END AS is_lowest
-- FROM 
--     source_data sd

select * from {{ref('dim_listings_cleansed')}}
where MINIMUM_nights <1 limit 10
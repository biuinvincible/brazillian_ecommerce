{{ config(materialized='table', schema='silver') }}
SELECT
    seller_id,
    COALESCE(seller_zip_code_prefix, '00000') as seller_zip_code_prefix,
    UPPER(TRIM(seller_city)) as seller_city,
    UPPER(TRIM(seller_state)) as seller_state
FROM {{ source('bronze', 'bronze_olist_sellers_dataset') }}
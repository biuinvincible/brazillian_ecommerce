-- models/silver/dim_seller.sql
{{ config(materialized='table', schema='gold') }}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ ref('silver_olist_sellers_dataset') }}
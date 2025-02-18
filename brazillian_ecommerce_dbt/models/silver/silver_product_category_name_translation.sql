{{ config(materialized='table', schema='silver') }}
SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('bronze', 'bronze_product_category_name_translation') }}
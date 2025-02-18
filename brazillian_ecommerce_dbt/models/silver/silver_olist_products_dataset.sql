{{ config(materialized='table', schema='silver') }}
SELECT
    product_id,
    product_category_name,
    COALESCE(product_name_lenght, 0) as product_name_length,
    COALESCE(product_description_lenght, 0) as product_description_length,
    COALESCE(product_photos_qty, 0) as product_photos_qty,
    COALESCE(product_weight_g, 0) as product_weight_g,
    COALESCE(product_length_cm, 0) as product_length_cm,
    COALESCE(product_height_cm, 0) as product_height_cm,
    COALESCE(product_width_cm, 0) as product_width_cm
FROM {{ source('bronze', 'bronze_olist_products_dataset') }}
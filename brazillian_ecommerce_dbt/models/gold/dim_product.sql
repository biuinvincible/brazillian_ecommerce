-- models/silver/dim_product.sql
{{ config(materialized='table', schema='gold') }}

SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
FROM {{ ref('silver_olist_products_dataset') }} p
LEFT JOIN {{ ref('silver_product_category_name_translation') }} t
ON p.product_category_name = t.product_category_name
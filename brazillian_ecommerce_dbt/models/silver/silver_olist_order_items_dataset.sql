{{ config(materialized='table', schema='silver') }}
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) as shipping_limit_date,
    CAST(price AS DECIMAL(10,2)) as price,
    CAST(freight_value AS DECIMAL(10,2)) as freight_value
FROM {{ source('bronze', 'bronze_olist_order_items_dataset') }}
{{ config(materialized='table', schema='silver') }}

SELECT
    order_id,
    customer_id,
    order_status,
    CAST(NULLIF(order_purchase_timestamp, '') AS TIMESTAMP) AS order_purchase_timestamp,
    CAST(NULLIF(order_approved_at, '') AS TIMESTAMP) AS order_approved_at,
    CAST(NULLIF(order_delivered_carrier_date, '') AS TIMESTAMP) AS order_delivered_carrier_date,
    CAST(NULLIF(order_delivered_customer_date, '') AS TIMESTAMP) AS order_delivered_customer_date,
    CAST(NULLIF(order_estimated_delivery_date, '') AS TIMESTAMP) AS order_estimated_delivery_date
FROM {{ source('bronze', 'bronze_olist_orders_dataset') }}
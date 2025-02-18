{{ config(materialized='table', schema='gold') }}

WITH timestamps AS (
    SELECT DISTINCT
        order_purchase_timestamp::timestamp AS timestamp
    FROM {{ ref('silver_olist_orders_dataset') }}
    WHERE order_purchase_timestamp IS NOT NULL
    UNION
    SELECT DISTINCT
        order_approved_at::timestamp AS timestamp
    FROM {{ ref('silver_olist_orders_dataset') }}
    WHERE order_approved_at IS NOT NULL
    UNION
    SELECT DISTINCT
        order_delivered_carrier_date::timestamp AS timestamp
    FROM {{ ref('silver_olist_orders_dataset') }}
    WHERE order_delivered_carrier_date IS NOT NULL
    UNION
    SELECT DISTINCT
        order_delivered_customer_date::timestamp AS timestamp
    FROM {{ ref('silver_olist_orders_dataset') }}
    WHERE order_delivered_customer_date IS NOT NULL
    UNION
    SELECT DISTINCT
        order_estimated_delivery_date::timestamp AS timestamp
    FROM {{ ref('silver_olist_orders_dataset') }}
    WHERE order_estimated_delivery_date IS NOT NULL
)

SELECT
    timestamp,
    EXTRACT(YEAR FROM timestamp) AS year,
    EXTRACT(MONTH FROM timestamp) AS month,
    EXTRACT(DAY FROM timestamp) AS day,
    EXTRACT(HOUR FROM timestamp) AS hour,
    EXTRACT(MINUTE FROM timestamp) AS minute,
    EXTRACT(SECOND FROM timestamp) AS second,
    TO_CHAR(timestamp, 'Day') AS day_of_week,
    TO_CHAR(timestamp, 'Month') AS month_name
FROM timestamps
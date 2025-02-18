{{ config(materialized='table', schema='gold') }}

WITH order_details AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp::timestamp AS order_purchase_timestamp,
        o.order_approved_at::timestamp AS order_approved_at,
        o.order_delivered_carrier_date::timestamp AS order_delivered_carrier_date,
        o.order_delivered_customer_date::timestamp AS order_delivered_customer_date,
        o.order_estimated_delivery_date::timestamp AS order_estimated_delivery_date,
        NULLIF(TRIM(oi.product_id), '') AS product_id,
        NULLIF(TRIM(oi.seller_id), '') AS seller_id,
        CAST(oi.price AS numeric) AS price,
        -- Fix: Remove TRIM for numeric column
        CAST(NULLIF(oi.freight_value, 0) AS numeric) AS freight_value,
        op.payment_type,
        op.payment_value,
        r.review_score,
        r.review_comment_title,
        r.review_comment_message
    FROM {{ ref('silver_olist_orders_dataset') }} o
    LEFT JOIN {{ ref('silver_olist_order_items_dataset') }} oi ON o.order_id = oi.order_id
    LEFT JOIN {{ ref('silver_olist_order_payments_dataset') }} op ON o.order_id = op.order_id
    LEFT JOIN {{ ref('silver_olist_order_reviews_dataset') }} r ON o.order_id = r.order_id
    WHERE o.order_purchase_timestamp IS NOT NULL
      AND o.order_approved_at IS NOT NULL
      AND o.order_estimated_delivery_date IS NOT NULL
      AND NULLIF(TRIM(oi.product_id), '') IS NOT NULL
      AND NULLIF(TRIM(oi.seller_id), '') IS NOT NULL
)

SELECT
    order_id,
    customer_id,
    seller_id,
    product_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    price,
    freight_value,
    payment_type,
    payment_value,
    review_score,
    review_comment_title,
    review_comment_message
FROM order_details
{{ config(materialized='table', schema='silver') }}
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS DECIMAL(10,2)) as payment_value
FROM {{ source('bronze', 'bronze_olist_order_payments_dataset') }}
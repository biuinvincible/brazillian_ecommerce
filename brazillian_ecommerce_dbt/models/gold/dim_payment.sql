-- models/silver/dim_payment.sql
{{ config(materialized='table', schema='gold') }}

SELECT DISTINCT
    payment_type
FROM {{ ref('silver_olist_order_payments_dataset') }}
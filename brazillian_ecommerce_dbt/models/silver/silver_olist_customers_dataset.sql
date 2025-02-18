{{ config(materialized='table', schema='silver') }}
SELECT
    customer_id,
    customer_unique_id,
    COALESCE(customer_zip_code_prefix, '00000') as customer_zip_code_prefix,
    UPPER(TRIM(customer_city)) as customer_city,
    UPPER(TRIM(customer_state)) as customer_state
FROM {{ source('bronze', 'bronze_olist_customers_dataset') }}
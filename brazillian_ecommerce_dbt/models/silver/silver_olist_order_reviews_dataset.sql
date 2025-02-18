{{ config(materialized='table', schema='silver') }}
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date AS TIMESTAMP) as review_creation_date,
    CAST(review_answer_timestamp AS TIMESTAMP) as review_answer_timestamp
FROM {{ source('bronze', 'bronze_olist_order_reviews_dataset') }}
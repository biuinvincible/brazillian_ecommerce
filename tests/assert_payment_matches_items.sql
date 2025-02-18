WITH order_items_total AS (
    SELECT order_id,
           SUM(price + freight_value) AS total_amount
    FROM {{ ref('silver_olist_order_items_dataset') }}
    GROUP BY order_id
),
order_payments_total AS (
    SELECT order_id,
           SUM(payment_value) AS total_payment
    FROM {{ ref('silver_olist_order_payments_dataset') }}
    GROUP BY order_id
)
SELECT *
FROM order_items_total i
JOIN order_payments_total p
  ON i.order_id = p.order_id
WHERE ABS(i.total_amount - p.total_payment) > 0.01

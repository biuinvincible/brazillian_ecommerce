SELECT *
FROM {{ ref('silver_olist_orders_dataset') }}
WHERE (order_delivered_customer_date IS NOT NULL AND order_delivered_customer_date < order_purchase_timestamp)
   OR (order_delivered_carrier_date IS NOT NULL AND order_delivered_carrier_date < order_purchase_timestamp)
   OR (order_approved_at IS NOT NULL AND order_approved_at < order_purchase_timestamp)
   OR (order_delivered_carrier_date IS NOT NULL AND order_approved_at IS NOT NULL 
       AND order_delivered_carrier_date < order_approved_at)
   OR (order_delivered_customer_date IS NOT NULL AND order_delivered_carrier_date IS NOT NULL 
       AND order_delivered_customer_date < order_delivered_carrier_date)

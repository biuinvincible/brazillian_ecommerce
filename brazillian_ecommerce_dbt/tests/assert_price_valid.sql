select * 
from {{ ref('silver_olist_order_items_dataset') }}
where price < 0
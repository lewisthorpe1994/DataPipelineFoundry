select customer_id, sum(order_total) as total_revenue
from {{ ref('silver_orders') }}
group by customer_id

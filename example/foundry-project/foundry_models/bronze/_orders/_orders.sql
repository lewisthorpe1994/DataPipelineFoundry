select
o.order_id,
o.customer_id,
o.order_date,
o.total_amount
from {{ source('some_orders','raw_orders') }} as o;

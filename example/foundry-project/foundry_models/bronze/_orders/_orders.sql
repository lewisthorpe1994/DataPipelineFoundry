-- create model bronze.bronze_orders as
--   drop table if exists bronze.bronze_orders cascade;
--   create table bronze.bronze_orders as
select
o.order_id,
o.customer_id,
o.order_date,
o.total_amount
from {{ source('some_orders','raw_orders') }} as o;

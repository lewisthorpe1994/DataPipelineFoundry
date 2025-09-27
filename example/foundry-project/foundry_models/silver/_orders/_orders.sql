-- create model silver.silver_orders as
--   drop table if exists silver.silver_orders cascade;
--   create table silver.silver_orders as
  select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount
  from {{ ref('bronze','orders') }} as o;

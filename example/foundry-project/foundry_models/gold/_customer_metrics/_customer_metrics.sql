-- create model gold.gold_customer_metrics as
--   drop view if exists gold.gold_customer_metrics cascade;
--   create view gold.gold_customer_metrics as
  select
    o.customer_id,
    count(*) as order_count,
    sum(o.total_amount) as total_revenue
  from {{ ref('silver','orders') }} as o
  group by o.customer_id;

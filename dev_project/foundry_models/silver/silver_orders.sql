select * from {{ ref('bronze_orders') }}
where order_date >= current_date - interval '30 days'

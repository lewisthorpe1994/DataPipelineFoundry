WITH payments AS (
    SELECT
        customer_id,
        date_of_payment,
        total_payment_amount
    FROM ref('silver', 'customer_agg_payments_by_day')
),

rentals AS (
    SELECT
        customer_id,
        date_of_rentals,
        total_rental_amount
    FROM ref('silver', 'customer_agg_rentals_by_day')
),

customer_daily_financials AS (
    SELECT
        COALESCE(p.customer_id, r.customer_id) AS customer_id,
        COALESCE(p.date_of_payment, r.date_of_rentals) AS activity_date,
        COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
        COALESCE(r.total_rental_amount, 0) AS total_rental_amount,
        COALESCE(p.total_payment_amount, 0) - COALESCE(r.total_rental_amount, 0) AS net_revenue
    FROM payments p
    FULL OUTER JOIN rentals r ON p.customer_id = r.customer_id AND p.date_of_payment = r.date_of_rentals
)

SELECT * FROM customer_daily_financials;

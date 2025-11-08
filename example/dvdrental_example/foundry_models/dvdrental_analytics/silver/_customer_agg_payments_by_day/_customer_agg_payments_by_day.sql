WITH payments_aggregated_by_day AS (
    SELECT
        customer_id,
        DATE_TRUNC('day', payment_date) AS date_of_payment,
        SUM(payment_amount) AS total_payment_amount
    FROM ref('silver', 'rental_customer')
    WHERE is_active
    GROUP BY customer_id, date_of_payment
)

SELECT * FROM payments_aggregated_by_day
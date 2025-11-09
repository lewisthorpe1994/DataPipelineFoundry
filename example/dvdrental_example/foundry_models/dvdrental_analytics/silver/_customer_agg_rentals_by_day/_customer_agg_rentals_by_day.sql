WITH rentals_aggregated_by_day AS (
    SELECT
        customer_id,
        DATE_TRUNC('day', rental_date) AS date_of_rentals,
        SUM(rental_amount) AS total_rental_amount
    FROM ref('silver', 'rental_customer')
    WHERE is_active
    GROUP BY customer_id, date_of_rentals
)

SELECT * FROM rentals_aggregated_by_day
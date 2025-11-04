WITH rental_customer AS (
    SELECT
        c.customer_id,
        f.film_id,
        f.film_title,
        f.rental_rate AS rental_amount,
        c.activebool AS is_active,
        r.rental_date,
        p.amount AS payment_amount,
        p.payment_date
    FROM ref('bronze', 'latest_customer') c
    LEFT JOIN ref('bronze', 'latest_rental') r ON c.customer_id = r.customer_id
    LEFT JOIN source('dvdrental_analytics', 'payment') p ON r.rental_id = p.rental_id
    LEFT JOIN ref('bronze', 'latest_inventory') i ON r.inventory_id = i.inventory_id
    LEFT JOIN ref('bronze', 'latest_film') f ON i.film_id = f.film_id
)

SELECT * FROM rental_customer
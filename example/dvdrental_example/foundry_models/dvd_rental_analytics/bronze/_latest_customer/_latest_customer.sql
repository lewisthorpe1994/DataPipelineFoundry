WITH latest AS (
    SELECT
        customer_id,
        MAX(last_update) AS last_update
    FROM source('dvd_rental_analytics', 'customer')
    GROUP BY customer_id
),

customer AS (
    SELECT
        customer_id,
        store_id,
        address_id,
        activebool,
        create_date,
        active AS is_active
    FROM source('dvd_rental_analytics', 'customer') f
    INNER JOIN latest l ON f.last_update = l.last_update AND f.customer_id = l.customer_id
)

SELECT * FROM customer;
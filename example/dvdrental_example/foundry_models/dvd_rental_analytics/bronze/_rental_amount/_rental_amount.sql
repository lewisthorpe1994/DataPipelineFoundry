WITH rental_amount AS (
    SELECT
        r.rental_id,
        rental_date,
        f.film_id,
        f.title as film_title,
        p.amount as rental_amount,
        p.payment_date
    FROM source('dvd_rental_analytics', 'rental') r
     LEFT JOIN source('dvd_rental_analytics', 'inventory') i ON r.inventory_id = i.inventory_id
     LEFT JOIN source('dvd_rental_analytics', 'film') f ON i.film_id = f.film_id
     LEFT JOIN source('dvd_rental_analytics','payment') p ON r.rental_id = p.rental_id
)

SELECT * FROM rental_amount;

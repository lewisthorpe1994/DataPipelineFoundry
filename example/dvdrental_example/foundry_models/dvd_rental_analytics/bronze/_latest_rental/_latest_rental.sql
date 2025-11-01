WITH latest AS (
    SELECT
        rental_id,
        MAX(last_update) AS max_last_update
    FROM source('dvd_rental_analytics', 'rental')
    GROUP BY rental_id
),
    
latest_rentals AS (
    SELECT
      rental_id,
      rental_date,
      inventory_id,
      customer_id,
      return_date,
      staff_id
    FROM source('dvd_rental_analytics', 'rental') r
    INNER JOIN latest l ON r.rental_id = l.rental_id AND r.rental_date = l.max_last_update
)

SELECT * FROM latest_rentals
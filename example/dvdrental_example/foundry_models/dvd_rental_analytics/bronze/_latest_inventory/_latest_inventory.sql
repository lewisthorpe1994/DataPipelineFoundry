WITH latest AS (
    SELECT
        inventory_id,
        MAX(last_update) AS last_update
    FROM source('dvd_rental_analytics', 'inventory')
    GROUP BY inventory_id
),

inventory AS (
     SELECT
        inventory_id,
        film_id,
        store_id
     FROM source('dvd_rental_analytics', 'inventory') f
     INNER JOIN latest l ON f.last_update = l.last_update AND f.inventory_id = l.inventory_id
 )

SELECT * FROM inventory;
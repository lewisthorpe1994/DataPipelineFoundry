WITH latest AS (
    SELECT
        film_id,
        MAX(last_update) AS last_update
    FROM source('dvdrental_analytics', 'film')
    GROUP BY film_id
),

film AS (
    SELECT
        f.film_id,
        title as film_title,
        title,
        description,
        release_year,
        language_id,
        rental_duration,
        rental_rate,
        length as film_length,
        replacement_cost,
        rating
    FROM source('dvdrental_analytics', 'film') f
    INNER JOIN latest l ON f.last_update = l.last_update AND f.film_id = l.film_id
)

SELECT * FROM film;

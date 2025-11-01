CREATE DATABASE dvdrental_analytics;

\connect dvdrental_analytics

CREATE SCHEMA IF NOT EXISTS raw
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE SEQUENCE IF NOT EXISTS raw_rental_rental_id_seq;
CREATE TABLE raw.rental
(
    rental_id    integer   DEFAULT nextval('raw_rental_rental_id_seq') PRIMARY KEY,
    rental_date  timestamp NOT NULL,
    inventory_id integer   NOT NULL,
    customer_id  smallint  NOT NULL,
    return_date  timestamp,
    staff_id     smallint  NOT NULL,
    last_update  timestamp DEFAULT now() NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS raw_film_film_id_seq;
CREATE TABLE raw.film
(
    film_id          integer       DEFAULT nextval('raw_film_film_id_seq') PRIMARY KEY,
    title            varchar(255)  NOT NULL,
    description      text,
    release_year     integer,
    language_id      smallint      NOT NULL,
    rental_duration  smallint      DEFAULT 3 NOT NULL,
    rental_rate      numeric(4, 2) DEFAULT 4.99 NOT NULL,
    length           smallint,
    replacement_cost numeric(5, 2) DEFAULT 19.99 NOT NULL,
    rating           varchar(10),
    last_update      timestamp     DEFAULT now() NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS raw_inventory_inventory_id_seq;
CREATE TABLE raw.inventory
(
    inventory_id integer   DEFAULT nextval('raw_inventory_inventory_id_seq') PRIMARY KEY,
    film_id      integer   NOT NULL,
    store_id     smallint  NOT NULL,
    last_update  timestamp DEFAULT now() NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS raw_customer_customer_id_seq;
CREATE TABLE raw.customer
(
    customer_id integer   DEFAULT nextval('raw_customer_customer_id_seq') PRIMARY KEY,
    store_id    smallint  NOT NULL,
    address_id  smallint  NOT NULL,
    activebool  boolean   DEFAULT TRUE NOT NULL,
    create_date date      DEFAULT current_date NOT NULL,
    last_update timestamp DEFAULT now(),
    active      integer
);

CREATE SEQUENCE IF NOT EXISTS raw_payment_payment_id_seq;
CREATE TABLE raw.payment
(
    payment_id   integer DEFAULT nextval('raw_payment_payment_id_seq') PRIMARY KEY,
    customer_id  smallint NOT NULL,
    staff_id     smallint NOT NULL,
    rental_id    integer,
    amount       numeric(5, 2) NOT NULL,
    payment_date timestamp    NOT NULL
);

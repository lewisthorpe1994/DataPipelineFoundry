FROM postgres:16

ENV POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres \
    POSTGRES_DB=dvdrental

RUN apt-get update \
&& apt-get install -y --no-install-recommends postgresql-16-decoderbufs \
&& rm -rf /var/lib/apt/lists/*

# Copy the directory-format pg_dump into the image
COPY . /docker-entrypoint-initdb.d/dvdrental
RUN chown -R postgres:postgres /docker-entrypoint-initdb.d/dvdrental

# Restore the dataset during init
COPY init-dvdrental.sh /docker-entrypoint-initdb.d/10-restore-dvdrental.sh
RUN chmod +x /docker-entrypoint-initdb.d/10-restore-dvdrental.sh

COPY 31-create-warehouse-raw-tables.sql /docker-entrypoint-initdb.d/31-create-warehouse-raw-tables.sql
RUN chown postgres:postgres /docker-entrypoint-initdb.d/31-create-warehouse-raw-tables.sql

# Enable logical replication prerequisites for Debezium
COPY 30-enable-logical-replication.sql /docker-entrypoint-initdb.d/30-enable-logical-replication.sql
RUN chown postgres:postgres /docker-entrypoint-initdb.d/30-enable-logical-replication.sql

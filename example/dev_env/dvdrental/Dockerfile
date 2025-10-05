FROM postgres:16

ENV POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres \
    POSTGRES_DB=dvdrental

# Copy the directory-format pg_dump into the image
COPY . /docker-entrypoint-initdb.d/dvdrental
RUN chown -R postgres:postgres /docker-entrypoint-initdb.d/dvdrental

# Restore the dataset during init
COPY init-dvdrental.sh /docker-entrypoint-initdb.d/10-restore-dvdrental.sh
RUN chmod +x /docker-entrypoint-initdb.d/10-restore-dvdrental.sh

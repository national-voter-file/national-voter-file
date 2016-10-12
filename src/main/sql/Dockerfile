FROM mdillon/postgis:9.5

ENV POSTGRES_DB VOTER

RUN  mkdir /sql
COPY ./clearDB.sql         /sql/
COPY ./create_database.sql /sql/
COPY ./create_tables.sql   /sql/
COPY ./populate_static_data.sql /sql/
COPY ./dockerResources/z-init-db.sh /docker-entrypoint-initdb.d/

EXPOSE 5432

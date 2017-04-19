FROM mdillon/postgis:9.5

ENV POSTGRES_DB VOTER

RUN  mkdir /sql
COPY ./dockerResources/z-init-db.sh /docker-entrypoint-initdb.d/

EXPOSE 5432

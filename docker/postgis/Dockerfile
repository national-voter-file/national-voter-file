FROM mdillon/postgis:9.5

ENV POSTGRES_DB VOTER

RUN  mkdir /sql
# COPY ../src/main/sql/clearDB.sql         /sql/
# COPY ../src/main/sql/create_database.sql /sql/
# COPY ../src/main/sql/create_tables.sql   /sql/
# COPY ../src/main/sql/populate_static_data.sql /sql/
COPY ./dockerResources/z-init-db.sh /docker-entrypoint-initdb.d/

EXPOSE 5432

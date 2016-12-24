#!/bin/bash

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -a --dbname=VOTER < /sql/create_database.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -a --dbname=VOTER < /sql/create_tables.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -a --dbname=VOTER < /sql/populate_static_data.sql

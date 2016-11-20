This directory contains the logical and physical data model for the voter file warehouse as well as generated DDL.

The data models are maintained using Oracle's [SQL Data Modeler Tool](http://www.oracle.com/technetwork/developer-tools/datamodeler/overview/index.html).

This model is based on standard data warehouse modelling techniques with an aim of making useful queries easy to generate while preserving history of the data. If you are unfamiliar with dimensional data modelling, you may enjoy [this article](https://dwbi.org/data-modelling/dimensional-model/1-dimensional-modeling-guide).

We assume that the movement app will maintain an Operational Data Store for transactional data related to administering and operating the application. This Online Analytic Processing database will perform the heavy lifting of historic analysis and voter identification.

We have a document describing the data architecture available as a [Google document](https://docs.google.com/document/d/169mIkiIdl4OetbGvnbVCzq9Srw9PKCsB6U1CErTD9aI/edit?usp=sharing).

# Setting up the Postgres Database (the hard way)

This ddl has been tested against Postgres version 9.5 with postgis extensions installed.

1. Execute create_database.sql to create the voter database instance
2. Execute create_tables.sql to run the DDL for setting up the DB
3. Run populate_static_date.sql to set up some static data in the dimensions.

The script clearDB.sql is handy for clearing out all data so you can run scripts over agains
with empty tables.

The DDL was initially generated from the SQLModeller data model, however significant adjustments are required to 
use it with Postgres. The file tablesNotYetReady.sql contains some tables that have not yet been cleaned up.
They will need some attention before they can be added to the DB.

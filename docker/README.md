# National Voter File Docker Compose
This directory contains docker resources to bring up your
very own national voter file.

We use [docker-compose](https://docs.docker.com/compose/) to manage interactions between the containers.  The apt repository may give you a different version of docker-compose that is incompatible with the configuration file, so be sure to get it from the link above.

To use this environment cd to this docker directory

1. Build the images with the command `% docker-compose build`
2. Launch the warehouse with the command `% docker-compose up`

## postgis
This container brings up an postgres instance with the postgis 
additions for GIS analysis. Once the database is up, it runs a startup script that creates the tables and loads some static data in. Finally, it exposes the postgres server through port 5432.

You can bind this port to your host and use your favorite SQL query tool to connect through this port.

Connection Information is as follows:

* driver=org.postgresql.Driver
* url=jdbc:postgresql://postgis:5432/VOTER
* user=postgres
* password=

## ETL
This container is intended to run [Pentaho Data Integration](http://community.pentaho.com/projects/data-integration/) transforms and python scripts with some handy modules. These scripts and transforms are how we enrich, clean, and load data into the postgres database.

Typically, you will invoke commands in this container as 
`% docker-compose run etl` 

## Running ETL Scripts
We've provided a shell script for loading in a 1,000 row sample of Washington state data. Take a look at buildWashington.sh for setting up a simple warehouse, or as guidance for running your own ETL jobs. It assumes that the voter file can be found in the data directory of this repo
(which is in .gitignore so you have to construct your own local version)

Copy those files into data/Washington directory of the national-voter-file repo

You can get samples of this data in our [private dropbox](https://www.dropbox.com/work/getmovement%20Team%20Folder) message us on the slack channel to get access.

### populateDateDimension.ktr
This transform creates records in the date dimension that has a row for every single date going out twenty years. This is required before loading any records into the warehouse.

### saveWashingtonPrecincts.ktr
This transform joins the Washington State precinct file with IDs from [Open Civic Data API](https://opencivicdata.readthedocs.io/en/latest/ocdids.html). This data is required before the voter file itself is loaded.


### ProcessWashingtonFile.kjb
This job glues together the steps required to load an entire voter file. For efficiency purposes, we run through the file once to extract any new households. We do it again to find new voter names.

Only after we perform this bulk load of households and voters do we go back and load all of the voter records which involves a lookup of this dimensional data.


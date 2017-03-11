# NVF with Docker

This directory contains docker resources to bring up a local national voter file.

We use [docker-compose](https://docs.docker.com/compose/) to manage interactions between the containers. This is already installed with Docker if you are using Windows or Mac.

## Getting started:

1. Move to the docker directory: `cd docker`
2. Build the docker containers: `docker-compose build`
3. Launch the warehouse with the command `docker-compose up`

4. Use your favorite (postgres-supporting) SQLing tool to connect (just fill in the ones your tool requires):

   * URL: `jdbc:postgresql://postgis:5432/VOTER`
   * Driver: `org.postgresql.Driver`
   * Host: `localhost`
   * Port: `5432`
   * Database: `VOTER`
   * User: `postgres`
   * Password: blank

Note: The host should be `postgis` when accessed from inside the ETL docker container.

## Loading Data

The `load` directory has controls for loading data, and is the main entrypoint for
the ETL container. Details on running this can be found here: [Loading Data](../load/README.md)

You can get samples of this data in our [private Dropbox](https://www.dropbox.com/work/getmovement%20Team%20Folder). Message us on Slack to get access.

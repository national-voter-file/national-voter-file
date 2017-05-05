# NVF Load Tools

Organization of utils for loading states using Pentaho and Python, with flexible
configuration. Setup local configuration for paths by copying `load_conf.json`
into `load_conf_local.json` which is ignored in git, and pass it to the loader
with the `--config_file` argument.

`loader.py` is also the entrypoint to the `etl` container in the Docker Compose
file, so if you want to run commands through this you can just add the arguments
to the end. Ex:

Instead of: `python loader.py transform -s wa --input_file=wa_test_file.csv`

Run: `docker-compose run etl transform -s wa --input_file=wa_test_file.csv`

## Initial Loading Test Data

* Load dates with `docker-compose run etl dates`
* **PENDING**: Load census data (after creating `dimensionaldata/census.csv` with
  the `getCensus.py` script) with `docker-compose run etl dimdata -s oh`
* Load precincts with `docker-compose run etl precincts -s oh --input_file=test/oh.csv`
* Run transformer with `docker-compose run etl transform -s oh --input_file=test/oh.csv`
* Load transformed data with `docker-compose run etl load -s oh --input_file=test/oh_output.csv --reporter_key=2`

## Use of loader in AWS Batch
We make extensive use of the loader in the AWS batch loading process. For this, we add a volume mount called `work`
where we put files that the transformer needs to intereact with such as the raw voter file and the transformed output.

We also put the config file in this directory so that it can hold connection info for the database in a secure place.
This gets used with the optional ``--create_jndi` option which causes a new simple jndi properties file to be created
inside the container's penthao installation. This jndi file has an entry for the name `voter` which has all of the
connection information needed for penthao to interact with the database.


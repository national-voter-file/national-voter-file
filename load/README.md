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

## Initial Loading

Using Michigan as an example:

* Load dates with `docker-compose run etl dates`
* Load jurisdictions with `docker-compose run etl dimdata -s mi`
* Load precincts with `docker-compose run etl precincts -s mi --report_date=2017-01-01 --input_file=mi/entire_state_v.lst`
* Run transformer with `docker-compose run etl transform -s mi --input_file=mi/mi.csv`
* Load transformed data with `docker-compose run etl load -s mi --input_file=mi/mi_output.csv --report_date=2017-01-01 --reporter_key=5`

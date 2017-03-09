# NVF Load Tools

Organization of utils for loading states using Pentaho and Python, with flexible
configuration.

## To-Dos

* Change naming conventions, move everything to lowercase state postal abbreviations
* More documentation
* Figure out flexibility on loading precinct files
* Potentially make loader the entrypoint to the etl container?


## Running Examples

* `docker-compose run etl python /national-voter-file/load/loader.py dimdata -s wa`
* `python loader.py transform -s wa --input_file=wa_test_file.csv`
* `python loader.py load -s wa --config_file=load_conf_prod.yml --report_date=2016-02-03 --reporter_key=1`

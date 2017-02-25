#!/bin/sh

##################
## Python script to clean up and enrich the Michigan Voter File
docker-compose run etl python3 /national-voter-file/src/python/national_voter_file/transformers/csv_transformer.py -s mi -d /national-voter-file/data/Michigan/FOIA_Voters.zip -o /national-voter-file/data/Washington


###################
## Run job to process Michigan voter file
docker-compose run etl /opt/pentaho/data-integration/kitchen.sh -file /national-voter-file/src/main/pdi/ProcessPreparedVoterFile.kjb -param:reportDate=2017-01-13 -param:reportFile=/national-voter-file/data/Michigan/mi_output.csv -param:reporterKey=4

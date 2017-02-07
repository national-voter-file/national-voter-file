#!/bin/sh

###################
## Populate the date dimension with 20 years worth
## of days
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/populateDateDimension.ktr

###################
## Store Washington State Jurisdictions into the Jurisdiction_dim table
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCensus.ktr -param:censusFile=/national-voter-file/dimensionaldata/census.csv -param:filterState=WA -param:lookupFile=/national-voter-file/dimensionaldata/countyLookup.csv

###################
## Load descriptions of every voting precinct in the state
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/washington/saveWashingtonPrecincts.ktr -param:reportDate=2016-05-30 -param:ocdFile=/national-voter-file/data/Washington/state-wa-precincts.csv -param:reportFile=/national-voter-file/data/Washington/2016.06.15-Districts_Precincts.csv

##################
## Python script to clean up and enrich the Washington Voter File
docker-compose run etl python3 /national-voter-file/src/python/national_voter_file/transformers/csv_transformer.py -s wa -d /national-voter-file/data/Washington/201605_VRDB_ExtractSAMPLE.txt -o /national-voter-file/data/Washington


###################
## Run job to process sample voter file
docker-compose run etl /opt/pentaho/data-integration/kitchen.sh -file /national-voter-file/src/main/pdi/ProcessPreparedVoterFile.kjb -param:reportDate=2016-05-30 -param:reportFile=/national-voter-file/data/Washington/wa_output.csv -param:reporterKey=1

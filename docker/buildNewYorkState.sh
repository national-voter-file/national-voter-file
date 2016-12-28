#!/bin/sh

###################
## Populate the date dimension with 20 years worth
## of days
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/populateDateDimension.ktr

################################
##### Load Electoral Jurisdiction Dimensions
################################
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCensus.ktr -param:censusFile=/national-voter-file/dimensionaldata/census.csv -param:filterState=NY -param:lookupFile=/national-voter-file/dimensionaldata/countyLookup.csv


##################
## Python script to clean up and enrich the New York Voter File
docker-compose run etl python3 /national-voter-file/src/main/python/transformers/newyork_prepare.py


###################
## Run job to process sample voter file
docker-compose run etl /opt/pentaho/data-integration/kitchen.sh -file /national-voter-file/src/main/pdi/newyork/ProcessNewYorkFile.kjb -param:reportDate=2016-08-31 -param:reportFile=/national-voter-file/data/NewYork/AllNYSVoters20160831SAMPLE_OUT.csv -param:reporterKey=3


docker-compose run etl /opt/pentaho/data-integration/pan.sh  -file /national-voter-file/src/main/pdi/UpdatePreparedHouseholdDimension.ktr -param:reportDate=2016-08-31 -param:reportFile=/national-voter-file/data/NewYork/AllNYSVoters20160831SAMPLE_OUT.csv -param:reporterKey=3

docker-compose run etl /opt/pentaho/data-integration/pan.sh  -file /national-voter-file/src/main/pdi/UpdatePreparedVoterAndPeopleDim.ktr -param:reportDate=2016-08-31 -param:reportFile=/national-voter-file/data/NewYork/AllNYSVoters20160831SAMPLE_OUT.csv -param:reporterKey=3

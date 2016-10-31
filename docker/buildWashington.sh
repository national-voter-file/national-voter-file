#!/bin/sh

################### 
## Populate the date dimension with 20 years worth
## of days
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/populateDateDimension.ktr

###################
## Load descriptions of every voting precinct in the state
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/washington/saveWashingtonPrecincts.ktr -param:reportDate=2016-05-30 -param:ocdFile=/national-voter-file/data/Washington/state-wa-precincts.csv -param:reportFile=/national-voter-file/data/Washington/2016.06.15-Districts_Precincts.csv 

###################
## Run job to process sample voter file
docker-compose run etl /opt/pentaho/data-integration/kitchen.sh -file /national-voter-file/src/main/pdi/washington/ProcessWashingtonFile.kjb -param:reportDate=2016-05-30 -param:reportFile=/national-voter-file/data/Washington/201605_VRDB_ExtractSAMPLE.txt -param:reporterKey=1



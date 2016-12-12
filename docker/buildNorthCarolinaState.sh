#!/bin/sh

################### 
## Populate the date dimension with 20 years worth
## of days
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/populateDateDimension.ktr

################################
##### Load Electoral Jurisdiction Dimensions
################################

## State Assembly
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadLowerHouseDim.ktr -param:fileName=//national-voter-file/dimensionaldata/stateHouse.csv 

## State Sentate
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadUpperHouseDim.ktr -param:fileName=/national-voter-file/dimensionaldata/stateSenate.csv 

## Congressional Districts
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCongressionalDistsDim.ktr -param:fileName=/national-voter-file/dimensionaldata/congress.csv 

## Counties
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCountyDim.ktr -param:censusFile=/national-voter-file/dimensionaldata/counties.csv -param:filterState=NC -param:lookupFile=/national-voter-file/src/main/pdi/northcarolina/countiesLookup.csv


##################
## Python script to clean up and enrich the New York Voter File
docker-compose run etl python3 /national-voter-file/src/main/python/NorthCarolinaPrepare.py /national-voter-file/data/NorthCarolina/ncvoter_Statewide.csv


###################
## Run job to process sample voter file
docker-compose run etl /opt/pentaho/data-integration/kitchen.sh -file /national-voter-file/src/main/pdi/northcarolina/ProcessNorthCarolinaFile.kjb -param:reportDate=2016-08-31 -param:reportFile=/national-voter-file/data/NorthCarolina/ncvoter_Statewide_OUT.csv -param:reporterKey=5


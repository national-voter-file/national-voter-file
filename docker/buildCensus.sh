#!/bin/sh

################### 
## Populate the date dimension with 20 years worth
## of days
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/populateDateDimension.ktr

################################
##### Load Electoral Jurisdiction Dimensions
################################

## State Assembly
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadLowerHouseDim.ktr -param:fileName=//national-voter-file/dimensionaldata/StateHouse.csv 

## State Sentate
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadUpperHouseDim.ktr -param:fileName=//national-voter-file/dimensionaldata/StateSenate.csv 

## Congressional Districts
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCongressionalDistsDim.ktr -param:fileName=//national-voter-file/dimensionaldata/congress.csv 

## Counties
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCountyDim.ktr -param:fileName=/national-voter-file/dimensionaldata/counties.csv 


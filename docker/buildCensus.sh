#!/bin/sh

################### 
## Populate the date dimension with 20 years worth
## of days
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file ../src/main/pdi/populateDateDimension.ktr

################################
##### Load Electoral Jurisdiction Dimensions
################################

## State Assembly
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file ../src/main/pdi/LoadLowerHouseDim.ktr -param:fileName=/../dimensionaldata/StateAssembly.txt 

## State Sentate
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file ../src/main/pdi/LoadUpperHouseDim.ktr -param:fileName=/../dimensionaldata/StateSenate.txt 

## Congressional Districts
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file ../src/main/pdi/LoadCongressionalDistsDim.ktr -param:fileName=/../dimensionaldata/congress.csv 

## Counties
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file ../src/main/pdi/LoadCountyDim.ktr -param:fileName=../dimensionaldata/counties.csv 


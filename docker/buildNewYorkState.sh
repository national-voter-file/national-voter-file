#!/bin/sh

################### 
## Populate the date dimension with 20 years worth
## of days
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/populateDateDimension.ktr

################################
##### Load Electoral Jurisdiction Dimensions
################################

## State Assembly
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadLowerHouseDim.ktr -param:fileName=/national-voter-file/dimensionaldata/NewYork/StateAssembly_OUT.txt 

## State Sentate
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadUpperHouseDim.ktr -param:fileName=/national-voter-file/dimensionaldata/NewYork/StateSenate_OUT.txt 

## Congressional Districts
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCongressionalDistsDim.ktr -param:fileName=/national-voter-file/dimensionaldata/NewYork/congressioal_dists_OUT.csv 

## New York Counties
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/LoadCountyDim.ktr -param:fileName=/national-voter-file/dimensionaldata/NewYork/counties_OUT.csv 


##################
## Python script to clean up and enrich the New York Voter File
docker-compose run etl python3 /national-voter-file/src/main/python/NewYorkPrepare.py /national-voter-file/data/NewYork/AllNYSVoters20160831SAMPLE.txt

###################
## Run job to process sample voter file
docker-compose run etl /opt/pentaho/data-integration/pan.sh -file /national-voter-file/src/main/pdi/newyork/SaveNewYorkReport.ktr -param:reportDate=2016-08-31 -param:reportFile=/national-voter-file/data/NewYork/AllNYSVoters20160831SAMPLE_OUT.csv -param:reporterKey=3



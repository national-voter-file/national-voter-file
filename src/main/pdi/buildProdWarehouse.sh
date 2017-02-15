
/opt/pentaho/data-integration/pan.sh -file /home/beng/git/national-voter-file/src/main/pdi/populateDateDimension.ktr

/opt/pentaho/data-integration/pan.sh -file /home/beng/git/national-voter-file/src/main/pdi/LoadCensus.ktr -param:censusFile=/home/beng/git/national-voter-file/dimensionaldata/census.csv -param:filterState=WA -param:lookupFile=/home/beng/git/national-voter-file/dimensionaldata/countyLookup.csv

/opt/pentaho/data-integration/pan.sh -file /home/beng/git/national-voter-file/src/main/pdi/washington/saveWashingtonPrecincts.ktr -param:reportDate=2016-05-30 -param:ocdFile=/mnt/vol-etl-nyc1-01/data/Washington/state-wa-precincts.csv -param:reportFile=/mnt/vol-etl-nyc1-01/data/Washington/2016/05/2016.06.15-Districts_Precincts.csv



nohup python3.5 national_voter_file/transformers/csv_transformer.py -s wa -d /mnt/vol-etl-nyc1-01/data/Washington/2016/05/201605_VRDB_Extract.txt -o /mnt/vol-etl-nyc1-01/data/Washington/2016/05

nohup /opt/pentaho/data-integration/kitchen.sh -file /home/beng/git/national-voter-file/src/main/pdi/ProcessPreparedVoterFile.kjb -param:reportDate=2016-05-30 -param:reportFile=/mnt/vol-etl-nyc1-01/data/Washington/2016/05/wa_output.csv -param:reporterKey=1 &

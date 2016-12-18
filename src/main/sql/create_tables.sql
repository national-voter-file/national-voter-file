  -- National Voter File Database Tables
-- This file contains the DDL for creating the tables, views, and indexes for
-- the national voter file data warehouse


DROP TABLE IF EXISTS DATE_DIM;
CREATE TABLE DATE_DIM
(
  DATE_ID INTEGER NOT NULL PRIMARY KEY
, DATE_VALUE DATE NOT NULL
, DATE_FULL VARCHAR(29)
, DATE_LONG VARCHAR(19)
, DATE_MEDIUM VARCHAR(15)
, DATE_SHORT VARCHAR(8)
, DAY_ABBREVIATION VARCHAR(3)
, DAY_IN_MONTH INTEGER
, DAY_IN_YEAR INTEGER
, DAY_NAME VARCHAR(9)
, MONTH_ABBREVIATION CHAR(3)
, MONTH_NAME VARCHAR(9)
, MONTH_NUMBER INTEGER
, QUARTER_NAME CHAR(2)
, QUARTER_NUMBER INTEGER
, WEEK_IN_MONTH INTEGER
, WEEK_IN_YEAR INTEGER
, YEAR2 CHAR(2)
, YEAR4 CHAR(4)
, YEAR_MONTH_ABBREVIATION CHAR(8)
, YEAR_MONTH_NUMBER CHAR(7)
, YEAR_QUARTER CHAR(7)
, IS_FIRST_DAY_IN_MONTH BOOLEAN
, IS_LAST_DAY_IN_MONTH BOOLEAN
, IS_LAST_DAY_IN_WEEK BOOLEAN
, IS_FIRST_DAY_IN_WEEK BOOLEAN
)
;
DROP INDEX IF EXISTS DATE_DIM_DATE_IDX;
CREATE UNIQUE INDEX DATE_DIM_DATE_IDX on DATE_DIM(DATE_VALUE);

DROP TABLE IF EXISTS REPORTER_DIM;
CREATE TABLE REPORTER_DIM
  (
    REPORTER_ID   SERIAL NOT NULL PRIMARY KEY ,
    REPORTER_NAME VARCHAR (50) ,
    REPORTER_TYPE VARCHAR (50)
  ) ;


DROP TABLE IF EXISTS PERSON_DIM;
CREATE TABLE PERSON_DIM
  (
    PERSON_ID     SERIAL NOT NULL PRIMARY KEY ,
    BIRTHDATE     DATE
  ) ;

DROP INDEX IF EXISTS PERSON_BIRTHDAY_IDX;
CREATE INDEX PERSON_BIRTHDAY_IDX on PERSON_DIM(BIRTHDATE);
DROP SEQUENCE IF EXISTS PERSON_SEQUENCE;
CREATE SEQUENCE PERSON_SEQUENCE;

DROP TABLE IF EXISTS VOTER_DIM;
CREATE TABLE VOTER_DIM
  (
    VOTER_ID            	SERIAL NOT NULL PRIMARY KEY,
    PERSON_KEY          	INTEGER NULL ,
    STATE_VOTER_REF   	VARCHAR (20) NULL ,
    COUNTY_VOTER_REF	VARCHAR(20) NULL,
    TITLE               	VARCHAR (5) NULL,
    FIRST_NAME               	VARCHAR (50) NULL ,
    MIDDLE_NAME               VARCHAR (50) NULL ,
    LAST_NAME               	VARCHAR (50) NULL ,
    NAME_SUFFIX          	VARCHAR (10) NULL ,
    GENDER              	CHAR (1) NULL,
    RACE                  CHAR(1) NULL,
    BIRTHDATE     		DATE NULL,
    BIRTH_STATE           CHAR(2) NULL,
    REGISTRATION_DATE   	DATE NULL,
    REGISTRATION_STATUS 	VARCHAR (15) NULL,
    ABSTENTEE_TYPE      	VARCHAR (1) NULL,
    PARTY               	VARCHAR (6) NULL,
    EMAIL               	VARCHAR (50) NULL,
    PHONE               	VARCHAR (15) NULL,
    DO_NOT_CALL_STATUS  	VARCHAR (1) NULL,
    LANGUAGE_CHOICE     	VARCHAR (3) NULL,
    VERSION		INTEGER NOT NULL DEFAULT(0),
    VALID_FROM		DATE NOT NULL DEFAULT('1900-01-01'),
    VALID_TO		DATE NOT NULL DEFAULT('2199-12-31')
  ) ;
DROP INDEX IF EXISTS STATE_VOTER_REF_IDX;
  CREATE INDEX STATE_VOTER_REF_IDX ON VOTER_DIM(STATE_VOTER_REF);

  DROP INDEX IF EXISTS VOTER_PERSON_KEY_IDX;
  CREATE INDEX VOTER_PERSON_KEY_IDX ON VOTER_DIM(PERSON_KEY);


DROP TABLE IF EXISTS HOUSEHOLD_DIM CASCADE;
CREATE TABLE HOUSEHOLD_DIM
  (
    HOUSEHOLD_ID                 SERIAL NOT NULL PRIMARY KEY,
    ADDRESS_NUMBER               VARCHAR(15) ,
    ADDRESS_NUMBER_PREFIX        VARCHAR(2) ,
    ADDRESS_NUMBER_SUFFIX        VARCHAR(5) ,
    BUILDING_NAME                VARCHAR(50) ,
    CORNER_OF                    VARCHAR(50) ,
    INTERSECTION_SEPARATOR       VARCHAR(5) ,
    LANDMARK_NAME                VARCHAR(50) ,
    NOT_ADDRESS                  VARCHAR(30) ,
    OCCUPANCY_TYPE               VARCHAR(20) ,
    OCCUPANCY_IDENTIFIER         VARCHAR(20) ,
    PLACE_NAME                   VARCHAR(50) ,
    STATE_NAME                   VARCHAR(15) ,
    STREET_NAME                  VARCHAR(50) ,
    STREET_NAME_PRE_DIRECTIONAL  VARCHAR(10) ,
    STREET_NAME_PRE_MODIFIER     VARCHAR(10) ,
    STREET_NAME_PRE_TYPE         VARCHAR(10) ,
    STREET_NAME_POST_DIRECTIONAL VARCHAR(10) ,
    STREET_NAME_POST_MODIFIER    VARCHAR(10) ,
    STREET_NAME_POST_TYPE        VARCHAR(10) ,
    SUBADDRESS_IDENTIFIER        VARCHAR(10) ,
    SUBADDRESS_TYPE              VARCHAR(10) ,
    USPS_BOX_GROUP_ID            VARCHAR(10) ,
    USPS_BOX_GROUP_TYPE          VARCHAR(2) ,
    USPS_BOX_ID                  VARCHAR(10) ,
    USPS_BOX_TYPE                VARCHAR(10) ,
    ZIP_CODE                     VARCHAR(10) ,
    GEOM                         GEOMETRY(Point, 4326) ,
    GEOCODE_STATUS               INTEGER NOT NULL DEFAULT(1) ,
    HASHCODE		   BIGINT NOT NULL
 ) ;
DROP INDEX IF EXISTS HOUSEHOLD_ZIP_IDX;
CREATE INDEX HOUSEHOLD_ZIP_IDX on HOUSEHOLD_DIM(ZIP_CODE);

DROP INDEX IF EXISTS HOUSEHOLD_HASH_IDX;
CREATE INDEX HOUSEHOLD_HASH_IDX on HOUSEHOLD_DIM(HASHCODE);

DROP INDEX IF EXISTS HOUSEHOLD_GEOM_IDX;
CREATE INDEX HOUSEHOLD_GEOM_IDX on HOUSEHOLD_DIM USING GIST(GEOM);

DROP TABLE IF EXISTS MAILING_ADDRESS_DIM CASCADE;
CREATE TABLE MAILING_ADDRESS_DIM
(
	MAILING_ADDRESS_ID 	SERIAL NOT NULL PRIMARY KEY
,	ADDRESS_LINE1	VARCHAR(110)
,	ADDRESS_LINE2	VARCHAR(50)
,	CITY		VARCHAR(50)
,	"STATE"		VARCHAR(20)
,	ZIP_CODE		VARCHAR(10)
,	COUNTRY   	VARCHAR(30)
,	HASHCODE		BIGINT NOT NULL
);
DROP INDEX IF EXISTS MAILING_ADDR_ZIP_IDX;
CREATE INDEX MAILING_ADDR_ZIP_IDX on MAILING_ADDRESS_DIM(ZIP_CODE);
DROP INDEX IF EXISTS MAILING_HASH_IDX;
CREATE INDEX MAILING_HASH_IDX on MAILING_ADDRESS_DIM(HASHCODE);


DROP TABLE IF EXISTS PRECINCT_DIM CASCADE;
CREATE TABLE PRECINCT_DIM(
  PRECINCT_ID SERIAL NOT NULL PRIMARY KEY
, COUNTY_CODE VARCHAR(5)
, COUNTY_NAME VARCHAR(50)  NULL
, DISTRICT_ID BIGINT  NULL
, DISTRICT_CODE VARCHAR(15)
, PRECINCT_CODE BIGINT
, PRECINCT_NAME VARCHAR(50)
, COUNTY_DISTRICT_NAME VARCHAR(100)
, EMS_DISTRICT_NAME VARCHAR(100)
, FIRE_DISTRICT_NAME VARCHAR(100)
, JUDICIAL_DISTRICT_NAME VARCHAR(100)
, LEGISLATIVE_DISTRICT_NAME VARCHAR(100)
, LIBRARY_DISTRICT_NAME VARCHAR(100)
, OTHER_DISTRICT_NAME VARCHAR(100)
, PCO_DISTRICT_NAME VARCHAR(100)
, PARK_AND_REC_DISTRICT_NAME VARCHAR(100)
, PORT_DISTRICT_NAME VARCHAR(100)
, PUBLIC_HOSPITAL_DISTRICT_NAME VARCHAR(100)
, PUBLIC_UTILITY_DISTRICT_NAME VARCHAR(100)
, SCHOOL_DISTRICT_NAME VARCHAR(100)
, SEWER_DISTRICT_NAME VARCHAR(100)
, STATE_DISTRICT_NAME VARCHAR(100)
, TAX_DISTRICT_NAME VARCHAR(100)
, TRANSPORTATION_DISTRICT_NAME VARCHAR(100)
, WATER_DISTRICT_NAME VARCHAR(100)
, STATE_ABBREVIATION CHAR(2)  NULL
, STATE_NAME CHAR(50)  NULL
, OCD_NAME VARCHAR(100)
, OCD_ID VARCHAR(100)
, VERSION		INTEGER NOT NULL DEFAULT(0)
, VALID_FROM		DATE NOT NULL DEFAULT('1900-01-01')
, VALID_TO		DATE NOT NULL DEFAULT('2199-12-31')

)
;


DROP TABLE IF EXISTS STAFFER_DIM;
CREATE TABLE STAFFER_DIM
  (
    STAFFER_ID INTEGER NOT NULL PRIMARY KEY,
    FIRST_NAME VARCHAR(45) ,
    LAST_NAME  VARCHAR(45) ,
    USERNAME   VARCHAR(25)
  );



DROP TABLE IF EXISTS ELECTION_DIM;
CREATE TABLE ELECTION_DIM
(
    ELECTION_ID SERIAL NOT NULL PRIMARY KEY
 ,  "STATE" CHAR(2)
,   ELECTION_DATE DATE
,   ELECTION_TYPE VARCHAR(10)
)
;

DROP TABLE IF EXISTS JURISDICTION_DIM;
CREATE TABLE public.JURISDICTION_DIM(
  jurisdiction_id SERIAL NOT NULL PRIMARY KEY,
  geoid character varying(12),
  fips character varying(5),
  voter_file_id character varying(5),
  voter_file_county_code character varying(2),
  state_name character varying(2),
  state_fips character varying(2),
  entity_fips character varying(3),
  entity_name character varying(75),
  entity_type character varying(5),
  total_pop integer,
  male_pop integer,
  maleunder_5_years integer,
  male5_to_9_years integer,
  male10_to_14_years integer,
  male15_to_17_years integer,
  male18_and_19_years integer,
  male20_years integer,
  male21_years integer,
  male22_to_24_years integer,
  male25_to_29_years integer,
  male30_to_34_years integer,
  male35_to_39_years integer,
  male40_to_44_years integer,
  male45_to_49_years integer,
  male50_to_54_years integer,
  male55_to_59_years integer,
  male60_and_61_years integer,
  male62_to_64_years integer,
  male65_and_66_years integer,
  male67_to_69_years integer,
  male70_to_74_years integer,
  male75_to_79_years integer,
  male80_to_84_years integer,
  male85_years_and_over integer,
  female_pop integer,
  femaleunder_5_years integer,
  female5_to_9_years integer,
  female10_to_14_years integer,
  female15_to_17_years integer,
  female18_and_19_years integer,
  female20_years integer,
  female21_years integer,
  female22_to_24_years integer,
  female25_to_29_years integer,
  female30_to_34_years integer,
  female35_to_39_years integer,
  female40_to_44_years integer,
  female45_to_49_years integer,
  female50_to_54_years integer,
  female55_to_59_years integer,
  female60_and_61_years integer,
  female62_to_64_years integer,
  female65_and_66_years integer,
  female67_to_69_years integer,
  female70_to_74_years integer,
  female75_to_79_years integer,
  female80_to_84_years integer,
  female85_years_and_over integer,
  not_hispanic_or_latino integer,
  not_hispanic_or_latinowhite_alone integer,
  not_hispanic_or_latinoblack_or_african_american_alone integer,
  not_hispanic_or_latinoamerican_indian_and_alaska_native_alone integer,
  not_hispanic_or_latinoasian_alone integer,
  not_hispanic_or_latinonative_hawaiian_and_other_pacific_islande integer,
  not_hispanic_or_latinosome_other_race_alone integer,
  not_hispanic_or_latinotwo_or_more_races integer,
  not_hispanic_or_latinotwo_or_more_racestwo_races_including_some integer,
  not_hispanic_or_latinotwo_or_more_racestwo_races_excluding_some integer,
  hispanic_or_latino integer,
  hispanic_or_latinowhite_alone integer,
  hispanic_or_latinoblack_or_african_american_alone integer,
  hispanic_or_latinoamerican_indian_and_alaska_native_alone integer,
  hispanic_or_latinoasian_alone integer,
  hispanic_or_latinonative_hawaiian_and_other_pacific_islander_al integer,
  hispanic_or_latinosome_other_race_alone integer,
  hispanic_or_latinotwo_or_more_races integer,
  hispanic_or_latinotwo_or_more_racestwo_races_including_some_oth integer,
  hispanic_or_latinotwo_or_more_racestwo_races_excluding_some_oth integer,
  never_married integer,
  now_married_except_separated integer,
  divorced integer,
  separated integer,
  widowed integer,
  born_in_state_of_residence integer,
  born_in_state_of_residencenever_married integer,
  born_in_state_of_residencenow_married_except_separated integer,
  born_in_state_of_residencedivorced integer,
  born_in_state_of_residenceseparated integer,
  born_in_state_of_residencewidowed integer,
  born_in_other_state_in_the_united_states integer,
  born_in_other_state_in_the_united_statesnever_married integer,
  born_in_other_state_in_the_united_statesnow_married_except_sepa integer,
  born_in_other_state_in_the_united_statesdivorced integer,
  born_in_other_state_in_the_united_statesseparated integer,
  born_in_other_state_in_the_united_stateswidowed integer,
  native_born_outside_the_united_states integer,
  native_born_outside_the_united_statesnever_married integer,
  native_born_outside_the_united_statesnow_married_except_separat integer,
  native_born_outside_the_united_statesdivorced integer,
  native_born_outside_the_united_statesseparated integer,
  native_born_outside_the_united_stateswidowed integer,
  foreign_born integer,
  foreign_bornnever_married integer,
  foreign_bornnow_married_except_separated integer,
  foreign_borndivorced integer,
  foreign_bornseparated integer,
  foreign_bornwidowed integer,
  income_less_than_10000 integer,
  income_10000_to_14999 integer,
  income_15000_to_19999 integer,
  income_20000_to_24999 integer,
  income_25000_to_29999 integer,
  income_30000_to_34999 integer,
  income_35000_to_39999 integer,
  income_40000_to_44999 integer,
  income_45000_to_49999 integer,
  income_50000_to_59999 integer,
  income_60000_to_74999 integer,
  income_75000_to_99999 integer,
  income_100000_to_124999 integer,
  income_125000_to_149999 integer,
  income_150000_to_199999 integer,
  income_200000_or_more integer,
  male16_to_19_years integer,
  male16_to_19_yearsin_labor_force integer,
  male16_to_19_yearsin_labor_forcein_armed_forces integer,
  male16_to_19_yearsin_labor_forcecivilian integer,
  male16_to_19_yearsin_labor_forcecivilianemployed integer,
  male16_to_19_yearsin_labor_forcecivilianunemployed integer,
  male16_to_19_yearsnot_in_labor_force integer,
  male20_and_21_yearsin_labor_force integer,
  male20_and_21_yearsin_labor_forcein_armed_forces integer,
  male20_and_21_yearsin_labor_forcecivilian integer,
  male20_and_21_yearsin_labor_forcecivilianemployed integer,
  male20_and_21_yearsin_labor_forcecivilianunemployed integer,
  male20_and_21_yearsnot_in_labor_force integer,
  male22_to_24_yearsin_labor_force integer,
  male22_to_24_yearsin_labor_forcein_armed_forces integer,
  male22_to_24_yearsin_labor_forcecivilian integer,
  male22_to_24_yearsin_labor_forcecivilianemployed integer,
  male22_to_24_yearsin_labor_forcecivilianunemployed integer,
  male22_to_24_yearsnot_in_labor_force integer,
  male25_to_29_yearsin_labor_force integer,
  male25_to_29_yearsin_labor_forcein_armed_forces integer,
  male25_to_29_yearsin_labor_forcecivilian integer,
  male25_to_29_yearsin_labor_forcecivilianemployed integer,
  male25_to_29_yearsin_labor_forcecivilianunemployed integer,
  male25_to_29_yearsnot_in_labor_force integer,
  male30_to_34_yearsin_labor_force integer,
  male30_to_34_yearsin_labor_forcein_armed_forces integer,
  male30_to_34_yearsin_labor_forcecivilian integer,
  male30_to_34_yearsin_labor_forcecivilianemployed integer,
  male30_to_34_yearsin_labor_forcecivilianunemployed integer,
  male30_to_34_yearsnot_in_labor_force integer,
  male35_to_44_yearsin_labor_force integer,
  male35_to_44_yearsin_labor_forcein_armed_forces integer,
  male35_to_44_yearsin_labor_forcecivilian integer,
  male35_to_44_yearsin_labor_forcecivilianemployed integer,
  male35_to_44_yearsin_labor_forcecivilianunemployed integer,
  male35_to_44_yearsnot_in_labor_force integer,
  male45_to_54_yearsin_labor_force integer,
  male45_to_54_yearsin_labor_forcein_armed_forces integer,
  male45_to_54_yearsin_labor_forcecivilian integer,
  male45_to_54_yearsin_labor_forcecivilianemployed integer,
  male45_to_54_yearsin_labor_forcecivilianunemployed integer,
  male45_to_54_yearsnot_in_labor_force integer,
  male55_to_59_yearsin_labor_force integer,
  male55_to_59_yearsin_labor_forcein_armed_forces integer,
  male55_to_59_yearsin_labor_forcecivilian integer,
  male55_to_59_yearsin_labor_forcecivilianemployed integer,
  male55_to_59_yearsin_labor_forcecivilianunemployed integer,
  male55_to_59_yearsnot_in_labor_force integer,
  male60_and_61_yearsin_labor_force integer,
  male60_and_61_yearsin_labor_forcein_armed_forces integer,
  male60_and_61_yearsin_labor_forcecivilian integer,
  male60_and_61_yearsin_labor_forcecivilianemployed integer,
  male60_and_61_yearsin_labor_forcecivilianunemployed integer,
  male60_and_61_yearsnot_in_labor_force integer,
  male62_to_64_yearsin_labor_force integer,
  male62_to_64_yearsin_labor_forcein_armed_forces integer,
  male62_to_64_yearsin_labor_forcecivilian integer,
  male62_to_64_yearsin_labor_forcecivilianemployed integer,
  male62_to_64_yearsin_labor_forcecivilianunemployed integer,
  male62_to_64_yearsnot_in_labor_force integer,
  male65_to_69_yearsin_labor_force integer,
  male65_to_69_yearsin_labor_forcecivilianemployed integer,
  male65_to_69_yearsin_labor_forcecivilianunemployed integer,
  male65_to_69_yearsnot_in_labor_force integer,
  male70_to_74_yearsin_labor_force integer,
  male70_to_74_yearsin_labor_forcecivilianemployed integer,
  male70_to_74_yearsin_labor_forcecivilianunemployed integer,
  male70_to_74_yearsnot_in_labor_force integer,
  male75_years_and_over integer,
  male75_years_and_overin_labor_force integer,
  male75_years_and_overin_labor_forcecivilianemployed integer,
  male75_years_and_overin_labor_forcecivilianunemployed integer,
  male75_years_and_overnot_in_labor_force integer,
  female integer,
  female16_to_19_yearsin_labor_force integer,
  female16_to_19_yearsin_labor_forcein_armed_forces integer,
  female16_to_19_yearsin_labor_forcecivilian integer,
  female16_to_19_yearsin_labor_forcecivilianemployed integer,
  female16_to_19_yearsin_labor_forcecivilianunemployed integer,
  female16_to_19_yearsnot_in_labor_force integer,
  female20_and_21_yearsin_labor_force integer,
  female20_and_21_yearsin_labor_forcein_armed_forces integer,
  female20_and_21_yearsin_labor_forcecivilian integer,
  female20_and_21_yearsin_labor_forcecivilianemployed integer,
  female20_and_21_yearsin_labor_forcecivilianunemployed integer,
  female20_and_21_yearsnot_in_labor_force integer,
  female22_to_24_yearsin_labor_force integer,
  female22_to_24_yearsin_labor_forcein_armed_forces integer,
  female22_to_24_yearsin_labor_forcecivilian integer,
  female22_to_24_yearsin_labor_forcecivilianemployed integer,
  female22_to_24_yearsin_labor_forcecivilianunemployed integer,
  female22_to_24_yearsnot_in_labor_force integer,
  female25_to_29_yearsin_labor_force integer,
  female25_to_29_yearsin_labor_forcein_armed_forces integer,
  female25_to_29_yearsin_labor_forcecivilian integer,
  female25_to_29_yearsin_labor_forcecivilianemployed integer,
  female25_to_29_yearsin_labor_forcecivilianunemployed integer,
  female25_to_29_yearsnot_in_labor_force integer,
  female30_to_34_yearsin_labor_force integer,
  female30_to_34_yearsin_labor_forcein_armed_forces integer,
  female30_to_34_yearsin_labor_forcecivilian integer,
  female30_to_34_yearsin_labor_forcecivilianemployed integer,
  female30_to_34_yearsin_labor_forcecivilianunemployed integer,
  female30_to_34_yearsnot_in_labor_force integer,
  female35_to_44_yearsin_labor_force integer,
  female35_to_44_yearsin_labor_forcein_armed_forces integer,
  female35_to_44_yearsin_labor_forcecivilian integer,
  female35_to_44_yearsin_labor_forcecivilianemployed integer,
  female35_to_44_yearsin_labor_forcecivilianunemployed integer,
  female35_to_44_yearsnot_in_labor_force integer,
  female45_to_54_yearsin_labor_force integer,
  female45_to_54_yearsin_labor_forcein_armed_forces integer,
  female45_to_54_yearsin_labor_forcecivilian integer,
  female45_to_54_yearsin_labor_forcecivilianemployed integer,
  female45_to_54_yearsin_labor_forcecivilianunemployed integer,
  female45_to_54_yearsnot_in_labor_force integer,
  female55_to_59_yearsin_labor_force integer,
  female55_to_59_yearsin_labor_forcein_armed_forces integer,
  female55_to_59_yearsin_labor_forcecivilian integer,
  female55_to_59_yearsin_labor_forcecivilianemployed integer,
  female55_to_59_yearsin_labor_forcecivilianunemployed integer,
  female55_to_59_yearsnot_in_labor_force integer,
  female60_and_61_yearsin_labor_force integer,
  female60_and_61_yearsin_labor_forcein_armed_forces integer,
  female60_and_61_yearsin_labor_forcecivilian integer,
  female60_and_61_yearsin_labor_forcecivilianemployed integer,
  female60_and_61_yearsin_labor_forcecivilianunemployed integer,
  female60_and_61_yearsnot_in_labor_force integer,
  female62_to_64_yearsin_labor_force integer,
  female62_to_64_yearsin_labor_forcein_armed_forces integer,
  female62_to_64_yearsin_labor_forcecivilian integer,
  female62_to_64_yearsin_labor_forcecivilianemployed integer,
  female62_to_64_yearsin_labor_forcecivilianunemployed integer,
  female62_to_64_yearsnot_in_labor_force integer,
  female65_to_69_yearsin_labor_force integer,
  female65_to_69_yearsin_labor_forcecivilianemployed integer,
  female65_to_69_yearsin_labor_forcecivilianunemployed integer,
  female65_to_69_yearsnot_in_labor_force integer,
  female70_to_74_yearsin_labor_force integer,
  female70_to_74_yearsin_labor_forcecivilianemployed integer,
  female70_to_74_yearsin_labor_forcecivilianunemployed integer,
  female70_to_74_yearsnot_in_labor_force integer,
  female75_years_and_over integer,
  female75_years_and_overin_labor_force integer,
  female75_years_and_overin_labor_forcecivilianemployed integer,
  female75_years_and_overin_labor_forcecivilianunemployed integer,
  female75_years_and_overnot_in_labor_force integer,
  owner_occupied integer,
  owner_occupied1person_household integer,
  owner_occupied2person_household integer,
  owner_occupied3person_household integer,
  owner_occupied4person_household integer,
  owner_occupied5person_household integer,
  owner_occupied6person_household integer,
  owner_occupied7ormore_person_household integer,
  renter_occupied integer,
  renter_occupied1person_household integer,
  renter_occupied2person_household integer,
  renter_occupied3person_household integer,
  renter_occupied4person_household integer,
  renter_occupied5person_household integer,
  renter_occupied6person_household integer,
  renter_occupied7ormore_person_household integer,
  median_value_dollars integer,
  version integer,
  valid_from date,
  valid_to date);


DROP TABLE IF EXISTS VOTER_REPORT_FACT;
CREATE TABLE VOTER_REPORT_FACT
  (
    VOTER_REPORT_ID       	SERIAL NOT NULL PRIMARY KEY ,
    VOTER_REPORT_DATE     	DATE NOT NULL,
    DATE_KEY		INTEGER NOT NULL REFERENCES DATE_DIM(DATE_ID),

    REPORT_STATUS         	VARCHAR(45) ,
    REPORTER_KEY          	INTEGER NOT NULL REFERENCES REPORTER_DIM(REPORTER_ID),
    VOTER_KEY	      	INTEGER NOT NULL REFERENCES VOTER_DIM(VOTER_ID),
    HOUSEHOLD_KEY	      	INTEGER NOT NULL REFERENCES HOUSEHOLD_DIM(HOUSEHOLD_ID) ,
    MAILING_ADDRESS_KEY	INTEGER NULL REFERENCES MAILING_ADDRESS_DIM(MAILING_ADDRESS_ID),
    SOCIAL_MEDIA_ACCOUNT_KEY 	INTEGER NULL ,
    PRECINCT_KEY INTEGER NULL REFERENCES PRECINCT_DIM(PRECINCT_ID),
    COUNTY_KEY  		INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    WARD_KEY	      	INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    CONGRESSIONAL_DIST_KEY 	INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    COUNTY_DISTRICT_KEY	INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    STATE_KEY                 INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    LOWER_HOUSE_DIST_KEY	INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    UPPER_HOUSE_DIST_KEY	INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    UNIFIED_SCHOOL_DIST_KEY	INTEGER NULL REFERENCES JURISDICTION_DIM(JURISDICTION_ID),
    STAFFER_KEY	          INTEGER NULL ,
    CAMPAIGN_KEY		INTEGER NULL
  ) ;




DROP TABLE IF EXISTS VOTE_FACT;
CREATE TABLE VOTE_FACT
  (
    VOTE_FACT_ID       	SERIAL NOT NULL PRIMARY KEY ,
    VOTER_KEY	      	INTEGER NOT NULL REFERENCES VOTER_DIM(VOTER_ID),
    HOUSEHOLD_KEY	      	INTEGER NOT NULL REFERENCES HOUSEHOLD_DIM(HOUSEHOLD_ID) ,
    PRECINCT_KEY INTEGER NULL REFERENCES PRECINCT_DIM(PRECINCT_ID),
    ELECTION_KEY INTEGER NOT NULL REFERENCES ELECTION_DIM(ELECTION_ID)
  ) ;


DROP TABLE IF EXISTS STAFFER_DIM;
CREATE TABLE STAFFER_DIM
  (
    STAFFER_ID INTEGER NOT NULL PRIMARY KEY,
    FIRST_NAME VARCHAR(45) ,
    LAST_NAME  VARCHAR(45) ,
    USERNAME   VARCHAR(25)
  );

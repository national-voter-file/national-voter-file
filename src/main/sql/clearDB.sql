
-- WARNING this script will delete all of your data!

truncate table VOTER_REPORT_FACT;
truncate table MAILING_ADDRESS_DIM CASCADE;
truncate table HOUSEHOLD_DIM CASCADE;
truncate table PERSON_DIM CASCADE;
truncate table VOTER_DIM CASCADE;


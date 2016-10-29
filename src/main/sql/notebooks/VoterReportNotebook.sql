

-- Common Table Expression Query to find the biggest change in registration counts
-- by city and zip
with may_voters as (
  select  place_name, zip_code, count(*) from VOTER_REPORT_FACT 
    join DATE_DIM on date_key=date_id
    join HOUSEHOLD_DIM on household_id=household_key
    where  month_name='May'
    group by place_name, zip_code
  ),
  july_voters as (
    select  place_name, zip_code, count(*) from VOTER_REPORT_FACT 
    join DATE_DIM on date_key=date_id
    join HOUSEHOLD_DIM on household_id=household_key
    where  month_name='July'
    group by place_name, zip_code
  )
  select may_voters.place_name, may_voters.zip_code, july_voters.count - may_voters.count net_gain, @(july_voters.count::numeric - may_voters.count)/may_voters.count change
    from may_voters inner join july_voters on may_voters.place_name=july_voters.place_name and may_voters.zip_code = july_voters.zip_code
    order by change desc;
  
  
  -- Find average number of voters in the household
   with household_counts as ( select household_id, count(*) from VOTER_REPORT_FACT
    join HOUSEHOLD_DIM on household_key=household_id
    join DATE_DIM on date_key=date_id
     where  month_name='July'
    group by household_id   
    ) select place_name, zip_code, avg(count) from household_counts hc
      join  household_dim hh on hc.household_id = hh.household_id
      group by hh.place_name, hh.zip_code
      limit 100;
    
select count(*) from household_dim;
select count(*) from voter_dim;
select count(*) from mailing_address_dim;

select * from mailing_address_dim limit 100;

select * from mailing_address_dim  where address_line2 is not null limit 200;

select count(*) from VOTER_REPORT_FACT where date_key=2398;

select month_name, count(*) from VOTER_REPORT_FACT join DATE_DIM on date_key=date_id group by month_name;
select * from household_dim limit 100;
select * from mailing_address_dim limit 100;
  
 select month_name, place_name, zip_code, count(*) from VOTER_REPORT_FACT 
  join DATE_DIM on date_key=date_id
  join HOUSEHOLD_DIM on household_id=household_key
  where zip_code in ('98001','98002','98092','98110')
  group by month_name, place_name, zip_code ;



select * from VOTER_REPORT_FACT
  join HOUSEHOLD_DIM on household_key=household_id
  join voter_dim on voter_key = voter_id
  limit 100;
  
  
  select household_id, count(*) from VOTER_REPORT_FACT
    join HOUSEHOLD_DIM on household_key=household_id
    group by household_id  having count(*) > 2 limit 100;
    
    
    select * from household_dim where household_id=63;
  
   select count(*) from voter_dim; 
   
   select distinct date_key from voter_report_fact;
   
   
   SELECT STATE_VOTER_REF, COUNT(*) FROM VOTER_DIM GROUP BY STATE_VOTER_REF HAVING COUNT(*) > 1 LIMIT 100;
   
   
-- Look at slicing voters by county/gender and pct of county's pop
with male_voters as 
  (select county_key, count(*) male_voter_count from voter_report_fact join voter_dim on voter_id=voter_key where gender='M' and reporter_key=3 group by county_key),
female_voters as 
  (select county_key, count(*) female_voter_count from voter_report_fact join voter_dim on voter_id=voter_key where gender='F' and reporter_key=3 group by county_key)
select * from county_dim
  join male_voters m on m.county_key=county_id
  join female_voters f on f.county_key=county_id;
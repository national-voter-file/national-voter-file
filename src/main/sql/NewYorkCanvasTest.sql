
insert into canvass_route_dim (route_title, creation_date, canvassing_date)
VALUES ('Brooklyn Canvas West', '2017-07-01', '2017-06-05'),
       ('Brooklyn Canvas East', '2017-07-01', '2017-06-05'),
       ('Brooklyn Canvas North', '2017-07-01', '2017-06-05'),
       ('Brooklyn Canvas South', '2017-07-01', '2017-06-05');

select * from canvass_route_dim;

insert into canvass_stop_fact ( voter_key, household_key, party_key, canvass_route_key)
  (select voter_key, household_key, party_key, 1
    from  voter_report_fact
      join household_dim on voter_report_fact.household_key = household_dim.household_id
    where  date_key = 2622
           and reporter_key = 3
           and raw_addr1= '9000 SHORE ROAD'
           and occupancy_identifier  like '%W%' limit 25);

insert into canvass_stop_fact ( voter_key, household_key, party_key, canvass_route_key)
  (select voter_key, household_key, party_key, 2
    from  voter_report_fact
      join household_dim on voter_report_fact.household_key = household_dim.household_id
    where  date_key = 2622
           and reporter_key = 3
           and raw_addr1= '9000 SHORE ROAD'
           and occupancy_identifier  like '%E%' limit 25);


insert into canvass_stop_fact ( voter_key, household_key, party_key, canvass_route_key)
  (select voter_key, household_key, party_key, 3
    from  voter_report_fact
      join household_dim on voter_report_fact.household_key = household_dim.household_id
    where  date_key = 2622
           and reporter_key = 3
           and raw_addr1= '9000 SHORE ROAD'
           and occupancy_identifier  like '%N%' limit 25);


insert into canvass_stop_fact ( voter_key, household_key, party_key, canvass_route_key)
  (select voter_key, household_key, party_key, 4
    from  voter_report_fact
      join household_dim on voter_report_fact.household_key = household_dim.household_id
    where  date_key = 2622
           and reporter_key = 3
           and raw_addr1= '9000 SHORE ROAD'
           and occupancy_identifier  like '%S%' limit 25);


select route_title,  state_voter_ref, party_name, raw_addr1, raw_addr2, first_name, last_name, middle_name
  from canvass_stop_fact
    join household_dim on canvass_stop_fact.household_key = household_dim.household_id
    join voter_dim on canvass_stop_fact.voter_key = voter_dim.voter_id
    join party_dim on canvass_stop_fact.party_key = party_dim.party_id
    join canvass_route_dim on canvass_stop_fact.canvass_route_key = canvass_route_dim.canvass_route_id;



-- Common Table Expression Query to find the biggest change in registration counts
-- by city and zip
WITH may_voters AS (
    SELECT
      place_name,
      zip_code,
      count(*)
    FROM VOTER_REPORT_FACT
      JOIN DATE_DIM ON date_key = date_id
      JOIN HOUSEHOLD_DIM ON household_id = household_key
    WHERE month_name = 'May'
    GROUP BY place_name, zip_code
),
    july_voters AS (
      SELECT
        place_name,
        zip_code,
        count(*)
      FROM VOTER_REPORT_FACT
        JOIN DATE_DIM ON date_key = date_id
        JOIN HOUSEHOLD_DIM ON household_id = household_key
      WHERE month_name = 'July'
      GROUP BY place_name, zip_code
  )
SELECT
  may_voters.place_name,
  may_voters.zip_code,
  july_voters.count - may_voters.count                                  net_gain,
  @(july_voters.count :: NUMERIC - may_voters.count) / may_voters.count change
FROM may_voters
  INNER JOIN july_voters
    ON may_voters.place_name = july_voters.place_name AND may_voters.zip_code = july_voters.zip_code
ORDER BY change DESC;

-- Find average number of voters in the household
WITH household_counts AS ( SELECT
                             household_id,
                             count(*)
                           FROM VOTER_REPORT_FACT
                             JOIN HOUSEHOLD_DIM ON household_key = household_id
                             JOIN DATE_DIM ON date_key = date_id
                           WHERE month_name = 'March'
                           GROUP BY household_id
) SELECT
    place_name,
    zip_code,
    avg(count)
  FROM household_counts hc
    JOIN household_dim hh ON hc.household_id = hh.household_id
  GROUP BY hh.place_name, hh.zip_code
  LIMIT 100;

SELECT count(*)
FROM household_dim;
SELECT count(*)
FROM voter_dim;
SELECT count(*)
FROM mailing_address_dim;

SELECT *
FROM mailing_address_dim
LIMIT 100;

SELECT *
FROM mailing_address_dim
WHERE address_line2 IS NOT NULL
LIMIT 200;

SELECT count(*)
FROM VOTER_REPORT_FACT
WHERE date_key = 2398;

SELECT
  month_name,
  count(*)
FROM VOTER_REPORT_FACT
  JOIN DATE_DIM ON date_key = date_id
GROUP BY month_name;
SELECT *
FROM household_dim
LIMIT 100;
SELECT *
FROM mailing_address_dim
LIMIT 100;

SELECT
  month_name,
  place_name,
  zip_code,
  count(*)
FROM VOTER_REPORT_FACT
  JOIN DATE_DIM ON date_key = date_id
  JOIN HOUSEHOLD_DIM ON household_id = household_key
WHERE zip_code IN ('98001', '98002', '98092', '98110')
GROUP BY month_name, place_name, zip_code;


SELECT *
FROM VOTER_REPORT_FACT
  JOIN HOUSEHOLD_DIM ON household_key = household_id
  JOIN voter_dim ON voter_key = voter_id
LIMIT 100;


SELECT
  household_id,
  count(*)
FROM VOTER_REPORT_FACT
  JOIN HOUSEHOLD_DIM ON household_key = household_id
GROUP BY household_id
HAVING count(*) > 2
LIMIT 100;


SELECT *
FROM household_dim
WHERE household_id = 63;

SELECT count(*)
FROM voter_dim;

SELECT DISTINCT date_key
FROM voter_report_fact;


SELECT
  STATE_VOTER_REF,
  COUNT(*)
FROM VOTER_DIM
GROUP BY STATE_VOTER_REF
HAVING COUNT(*) > 1
LIMIT 100;

-- Look at slicing voters by county/gender and pct of county's pop
WITH male_voters AS
(SELECT
   county_key,
   count(*) male_voter_count
 FROM voter_report_fact
   JOIN voter_dim ON voter_id = voter_key
 WHERE gender = 'M' AND reporter_key = 3
 GROUP BY county_key),
    female_voters AS
  (SELECT
     county_key,
     count(*) female_voter_count
   FROM voter_report_fact
     JOIN voter_dim ON voter_id = voter_key
   WHERE gender = 'F' AND reporter_key = 3
   GROUP BY county_key)
SELECT *
FROM county_dim
  JOIN male_voters m ON m.county_key = county_id
  JOIN female_voters f ON f.county_key = county_id;

-- Analyze rejected Ballots by county
WITH election AS (
    SELECT election_id
    FROM election_dim
    WHERE election_type = 'General' AND election_date = '2016-11-08'),

    votes_cast AS (
      SELECT
        county_key,
        CAST(count(*) AS FLOAT) AS votes_cast
      FROM vote_fact
        JOIN election ON election_id = vote_fact.election_key
      WHERE did_vote = 'Y'
      GROUP BY county_key
  ),
    votes_rejected AS (
      SELECT
        county_key,
        CAST(count(*) AS FLOAT) AS votes_rejected
      FROM vote_fact
        JOIN election ON election_id = vote_fact.election_key
      WHERE vote_counted = 'N'
      GROUP BY county_key
  ),
    registered_voters AS (
      SELECT
        county_key,
        CAST(count(*) AS FLOAT) AS registered_voters
      FROM voter_report_fact
        JOIN voter_dim ON voter_id = voter_key
      WHERE reporter_key = 4 AND date_key = 2623
      GROUP BY county_key
  )
SELECT
  entity_name,
  total_pop,
  registered_voters,
  votes_cast,
  votes_rejected,
  votes_rejected / votes_cast    AS pct_votes_rejected,
  votes_cast / registered_voters AS voter_turnout
FROM votes_cast
  JOIN votes_rejected ON votes_cast.county_key = votes_rejected.county_key
  JOIN registered_voters ON votes_cast.county_key = registered_voters.county_key
  JOIN jurisdiction_dim ON votes_cast.county_key = jurisdiction_dim.jurisdiction_id
ORDER BY pct_votes_rejected DESC;
SELECT *
FROM party_dim;

SELECT
  party_name,
  lower_house.entity_name,
  count(1)
FROM voter_report_fact
  JOIN party_dim ON party_id = party_key
  JOIN jurisdiction_dim lower_house ON lower_house.jurisdiction_id = voter_report_fact.lower_house_dist_key
WHERE reporter_key = 4
GROUP BY party_name, lower_house.entity_name
ORDER BY lower_house.entity_name;




WITH voter_summary AS (
    SELECT
      county_key,
      gender,
      DATE_PART('year', voter_report_date) - DATE_PART('year', birthdate) "age",
      voter_report_date - registration_date                               "days registered",
      party_code
    FROM voter_report_fact
      JOIN voter_dim ON voter_report_fact.voter_key = voter_dim.voter_id
      JOIN party_dim ON voter_report_fact.party_key = party_dim.party_id
    WHERE reporter_key = 3 AND date_key = 2622 AND registration_status IN ('ACTIVE', 'ACT')),
    grand_total AS (SELECT
                      county_key,
                      count(*)
                    FROM voter_summary
                    GROUP BY county_key),
    male_totals AS (SELECT
                      county_key,
                      count(*) "Males"
                    FROM voter_summary
                    WHERE gender = 'M'
                    GROUP BY county_key),
    female_totals AS (SELECT
                        county_key,
                        count(*) "Females"
                      FROM voter_summary
                      WHERE gender = 'F'
                      GROUP BY county_key),
    age_18_24 AS (SELECT
                    county_key,
                    count(*) "age_18_24"
                  FROM voter_summary
                  WHERE age < 25
                  GROUP BY county_key),
    age_25_34 AS (SELECT
                    county_key,
                    count(*) "age_25_34"
                  FROM voter_summary
                  WHERE age BETWEEN 25 AND 34
                  GROUP BY county_key),
    age_35_44 AS (SELECT
                    county_key,
                    count(*) "age_35_44"
                  FROM voter_summary
                  WHERE age BETWEEN 35 AND 44
                  GROUP BY county_key),
    age_45_54 AS (SELECT
                    county_key,
                    count(*) "age_45_54"
                  FROM voter_summary
                  WHERE age BETWEEN 45 AND 54
                  GROUP BY county_key),
    age_55_64 AS (SELECT
                    county_key,
                    count(*) "age_55_64"
                  FROM voter_summary
                  WHERE age BETWEEN 55 AND 64
                  GROUP BY county_key),
    age_gt_64 AS (SELECT
                    county_key,
                    count(*) "age_gt_64"
                  FROM voter_summary
                  WHERE age > 64
                  GROUP BY county_key),
    democrats AS (SELECT
                    county_key,
                    count(*) "democrats"
                  FROM voter_summary
                  WHERE party_code = 'DEM'
                  GROUP BY county_key),
    republicans AS (SELECT
                      county_key,
                      count(*) "republicans"
                    FROM voter_summary
                    WHERE party_code = 'REP'
                    GROUP BY county_key),
    inactive AS (SELECT
                   county_key,
                   count(*) "inactive"
                 FROM voter_report_fact
                   JOIN voter_dim ON voter_report_fact.voter_key = voter_dim.voter_id
                 WHERE reporter_key = 3 AND date_key = 2622 AND registration_status IN ('INACTIVE', 'INA')
                 GROUP BY county_key),
    purged AS (SELECT
                 county_key,
                 count(*) "purged"
               FROM voter_report_fact
                 JOIN voter_dim ON voter_report_fact.voter_key = voter_dim.voter_id
               WHERE reporter_key = 3 AND date_key = 2622 AND registration_status IN ('PURGED')
               GROUP BY county_key)

SELECT *
FROM grand_total
  JOIN male_totals ON grand_total.county_key = male_totals.county_key
  JOIN female_totals ON grand_total.county_key = female_totals.county_key
  JOIN age_18_24 ON grand_total.county_key = age_18_24.county_key
  JOIN age_25_34 ON grand_total.county_key = age_25_34.county_key
  JOIN age_35_44 ON grand_total.county_key = age_35_44.county_key
  JOIN age_45_54 ON grand_total.county_key = age_45_54.county_key
  JOIN age_55_64 ON grand_total.county_key = age_55_64.county_key
  JOIN age_gt_64 ON grand_total.county_key = age_gt_64.county_key
  JOIN democrats ON grand_total.county_key = democrats.county_key
  JOIN republicans ON grand_total.county_key = republicans.county_key
  JOIN inactive ON grand_total.county_key = inactive.county_key
  JOIN purged ON grand_total.county_key = purged.county_key;

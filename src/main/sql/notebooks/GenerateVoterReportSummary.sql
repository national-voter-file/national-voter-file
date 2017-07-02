INSERT INTO voter_report_summary (reporter_key,
report_date_key,

state_code,
total,
inactive,
purged,
male,
female,
age_18_24,
age_25_34,
age_35_44,
age_45_54,
age_55_64,
age_gt_64,
democrat,
republican,
other_party,
unaffiliated,
registered_in_last_90_days) (
WITH voter_summary AS (
    SELECT
      county_key,
      gender,
      DATE_PART('year', voter_report_date) - DATE_PART('year', birthdate) "age",
      voter_report_date - registration_date                               "days registered",
      party_code,
      reporter_key, date_key
    FROM voter_report_fact
      JOIN voter_dim ON voter_report_fact.voter_key = voter_dim.voter_id
      JOIN party_dim ON voter_report_fact.party_key = party_dim.party_id
    WHERE reporter_key = 3 AND date_key = 2622 AND registration_status IN ('ACTIVE', 'ACT')),
    grand_total AS (SELECT
                      county_key,
                      count(*) "grand_total"
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
    registered_90_days AS (SELECT
                    county_key,
                    count(*) "registered_90_days"
                  FROM voter_summary
                  WHERE "days registered" <= 90
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
    other_party AS (SELECT
                      county_key,
                      count(*) "other_party"
                    FROM voter_summary
                    WHERE party_code not in ('REP', 'DEM')
                    GROUP BY county_key),
    unafiliated AS (SELECT
                      county_key,
                      count(*) "unafiliated"
                    FROM voter_summary
                    WHERE party_code = 'UN'
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

SELECT  3 "reporter_key", 2622 "date_key", grand_total.county_key "jurisdiction", 'NY' "state_code",
  grand_total.grand_total, inactive, purged, male_totals."Males", female_totals."Females", age_18_24.age_18_24,
  age_25_34.age_25_34, age_35_44, age_45_54, age_55_64, age_gt_64,
  democrats, republicans, other_party, unafiliated, registered_90_days
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
  JOIN other_party ON grand_total.county_key = other_party.county_key
  JOIN unafiliated ON grand_total.county_key = unafiliated.county_key
  JOIN registered_90_days on grand_total.county_key = registered_90_days.county_key
  JOIN inactive ON grand_total.county_key = inactive.county_key
  JOIN purged ON grand_total.county_key = purged.county_key);

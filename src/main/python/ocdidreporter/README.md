# OCD-ID Reporter

This directory has a _.gitignore_ to ignore the _*.db_ SQLite3 database file
(about 80 MB) that is created once the script is run.


## pull_ocdid_data.py

This is a Python script that pulls OCD-ID data from the
[opencivicdata/ocd-division-ids GitHub repo][ocd_repo]
and loads it into a SQLite3 database for inspection.

Usage:

```
python pull_ocdid_data.py
```

Or from _national-voter-file/docker_
(which puts the database in the _data/_ directory):

```
docker-compose run etl \
  python /national-voter-file/src/main/python/ocdidreporter/pull_ocdid_data.py
```


## oc-id.sqlite.db

The database can be accessed using [sqlite][sqlite],
which is bundled with Python's standard library, so
you should be able to just type `sqlite3 ocdid.sqlite.db`
on the command line and use the database:

```
sqlite3 ocdid.sqlite.db
.help
.tables
.schema country_us
```

Both tables are keyed on the (string-formatted, sorry) `ocdid` column.
the `lookup` table contains the ocdid broken up into its constituent parts,
so you can do stuff like:

```sqlite> select count(1) from lookup where state='ny';
3561
```

The `country_us` table contains the exact contents of the
_country-us.csv_ file, (from the opencivicdata GitHub repo),
but with the database you can now do stuff to inspect the file like:

```
sqlite> SELECT cu.* 
   FROM country_us AS cu JOIN lookup AS l
   ON cu.ocdid = l.ocdid
   WHERE state='ny' AND school_district IS NOT NULL
   LIMIT 3;
ocd-division/country:us/state:ny/county:albany/school_district:achievement_academy_charter_school|achievement academy charter school||||3600129|||||010100860876||
ocd-division/country:us/state:ny/county:albany/school_district:albany_city_school_district|albany city school district||||3602460|||||010100010000||
ocd-division/country:us/state:ny/county:albany/school_district:albany_community_charter_school|albany community charter school||||3600162|||||010100860899||
```


### SQLite3

- To exit, type `.quit`
- For help, type `.help`
- To see tables, type `.tables`
- To see the definition for a table, type `.schema <tablename>`


### OCDEP 2: Open Civic Data Divisions
This script reads the division IDs from
_identifiers/country-us.csv_ in the repo
https://github.com/opencivicdata/ocd-division-ids/,
and loads them into a sqlite database named _ocd-id.db_,
under the table name `country_us`.

In a second table, named `lookup`, the OCD-ID types are
broken into columns to be joined to. The goal is to make
a lookup table that maps from the column name and value in
each state's voter file to the correct OCD-ID.
  
The OCD-IDs are globally unique identifiers for political divisions.
- Defined in:
..* http://docs.opencivicdata.org/en/latest/proposals/0002.html
- Implemented in:
..* https://github.com/opencivicdata/ocd-division-ids
- Identifier format: 
..* ocd-division/country:<country_code>(/<type>:<type_id>)*


[ocd_repo]: https://github.com/opencivicdata/ocd-division-ids/blob/master/identifiers/country-us.csv
[sqlite]: https://www.sqlite.org

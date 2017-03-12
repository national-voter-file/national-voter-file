"""
pull_ocdid_data.py

usage:
    python pull_ocdid_data.py

creates an SQLite3 database:
    ocd-id.sqlite.db


This script OCD-ID data from the opencivicdata/ocd-division-ids
GitHub repo* and loads it into a SQLite3 database for inspection.

  *https://github.com/opencivicdata/ocd-division-ids/
      blob/master/identifiers/country-us.csv


The datasets:

  OCDEP 2: Open Civic Data Divisions
      This script reads the division IDs from the file
      _identifiers/country-us.csv_ in this repo,
      https://github.com/opencivicdata/ocd-division-ids/,
      and loads them into a sqlite database named _ocd-id.db_,
      under the table name `country_us`.
  
      In a second table, named `lookup`, the OCD-ID types are
      broken into columns to be joined to. The goal is to make
      a lookup table that maps from the column name and value in
      each state's voter file to the correct OCD-ID.
  
  The OCD-IDs are flobally unique identifiers for political divisions.
      Defined in:
          http://docs.opencivicdata.org/en/latest/proposals/0002.html
      Implemented in:
          https://github.com/opencivicdata/ocd-division-ids
      Identifier format: 
          ocd-division/country:<country_code>(/<type>:<type_id>)*
"""
from __future__ import print_function
import collections
#import csv
import io
import os
import sqlite3
try:
    import urllib.request as request  # Python 3
    from csv import DictReader

    def get_iostream(response):
        return io.StringIO(response.read().decode('utf-8'))

    def utf8(text):
        return text
    
except ImportError:
    import urllib2 as request  # Python 2
    import codecs
    from csv import DictReader #import csv

    def get_iostream(response):
        return io.BytesIO(response.read())

    def utf8(text):
        return unicode(text, 'utf-8')


OCDID_US_DATA_URI = (
    'https://github.com/opencivicdata/ocd-division-ids/'
    'raw/master/identifiers/country-us.csv'
)

running_in_docker = os.path.exists('/.dockerenv')
if running_in_docker:
    DATABASE_NAME = '/national-voter-file/data/ocd-id.sqlite.db'
else:
    DATABASE_NAME = 'ocd-id.sqlite.db'


print('Downloading from\n', OCDID_US_DATA_URI)
response = request.urlopen(OCDID_US_DATA_URI)
iostream = get_iostream(response)
rdr = DictReader(iostream)
fieldnames = rdr.fieldnames
all_rows = [row for row in rdr]
ids = [row['id'] for row in all_rows]


# First, put the whole dataset into sqlite3 as it is.
# Create the table
print('Writing to', DATABASE_NAME)
conn = sqlite3.connect(DATABASE_NAME)
c = conn.cursor()
c.execute("""
    CREATE TABLE IF NOT EXISTS
    country_us (ocdid TEXT PRIMARY KEY
        ,{},
        CONSTRAINT unique_ocdid UNIQUE (ocdid) ON CONFLICT IGNORE
    );
    """.format('\n        ,'.join(
        '{} TEXT'.format(k) for k in fieldnames[1:]))
)

# Populate the table
insertion = """ 
    INSERT INTO country_us
    (ocdid\n,{})
    VALUES ({})
""".format('\n,'.join(fieldnames[1:]), ', '.join(['?'] * len(fieldnames)))
c.executemany(
    insertion,
    [tuple(utf8(row[f]) for f in fieldnames) for row in all_rows])


# Now, get the hierarchy of region types by looking at the first column.
# OCD-ID values are of the form 
#   ocd-division/country:<country_code>(/<type>:<type_id>)*
splits = [id.split('/') for id in ids]
types = [(s[2], tuple(sub.split(':')[0] for sub in s[2:])) for s in splits if len(s) > 2]
type_hierarchy = {}
for locale, entry in types:
    if locale not in type_hierarchy:
        type_hierarchy[locale] = {}
    sub_type = type_hierarchy[locale]
    for type in entry:
        if type not in sub_type:
            sub_type[type] = {'COUNT': 0}
        sub_type[type]['COUNT'] += 1
        sub_type = sub_type[type]


# -----------------------
# Get all of the possible columns in the dataset
tmp = set([sub.split(':')[0] for s in splits if len(s) > 2 for sub in s[2:]])
all_possible_columns = sorted([s.lower() for s in tmp])

c.execute("""
    CREATE TABLE IF NOT EXISTS
    lookup (
        ocdid TEXT PRIMARY KEY
        ,{},
        CONSTRAINT unique_lookup_ocid UNIQUE (ocdid) ON CONFLICT IGNORE
    );
    """.format('\n        ,'.join(
            '{} TEXT'.format(k) for k in all_possible_columns))
)

insertion_template = """ 
    INSERT INTO lookup
    (ocdid, {})
    VALUES ('{}', {})
"""

for id, split in zip(ids, splits):
    if len(split) > 2:
        all_keys, all_vals = zip(*[utf8(s).split(':') for s in split[2:]])
        insertion = insertion_template.format(
            ', '.join(all_keys),
            id,
            ','.join(['?'] * len(all_vals))
        )
        c.execute(insertion, all_vals)

conn.commit()
conn.close()

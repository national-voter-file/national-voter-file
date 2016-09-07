# Query from database to get address data

# Contributers: Mike Bruno bruno615@gmail.com

import psycopg2
import pdb
from secret import DATABASES
import pandas

# Requires a secret.py file in the same directory containing database connection info

connection_database_name = 'DevVoter'

# Connect to database and return a db_cursor, which can be used for querying
# Database name is the name of the database as specified in the secret.py file
def connect_to_db(connection_database_name):
  try:
    DBconnR = psycopg2.connect(**DATABASES[connection_database_name])
    db_cursor = DBconnR.cursor()
  except Exception as ex:
    print 'Connection error with %s: %s' % (connection_database_name, ex)
  return(db_cursor)

# Runs single query using a database cursor and query text
def run_query(db_cursor, query):
  # Query the DB
  db_cursor.execute(query)
  results = db_cursor.fetchall()
  # Convert into a Pandas Dataframe
  colnames = [desc[0] for desc in db_cursor.description]
  dataframe = pandas.DataFrame(results, columns = colnames)

  return(dataframe)

# fetch address records to query.  Limit = number of rows requested
def get_unmatched_address_records(limit = 1):

# TODO:  Add geo_match_status to table and uncomment
  query_address = '''
    select
      ADDRESS_NUMBER_PREFIX,
      ADDRESS_NUMBER,
      ADDRESS_NUMBER_SUFFIX,
      STREET_NAME_PRE_DIRECTIONAL,
      STREET_NAME_PRE_MODIFIER,
      STREET_NAME_PRE_TYPE,
      STREET_NAME,
      STREET_NAME_POST_TYPE,
      STREET_NAME_POST_MODIFIER,
      STREET_NAME_POST_DIRECTIONAL,
      OCCUPANCY_TYPE,
      OCCUPANCY_IDENTIFIER
    from household_dim
    /* where geo_match_status <> 'success' */
    limit %s
    ''' % limit

  # Get the database cursor
  db_cursor = connect_to_db(connection_database_name)

  # Run the address query
  result = run_query(db_cursor, query_address)

  # Convert to JSON for output
  result = result.to_json(orient = 'records')
  return(result)

#print(get_unmatched_address_records(1))




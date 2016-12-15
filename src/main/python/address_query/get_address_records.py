# Query from database to get address data

# Contributers: Mike Bruno bruno615@gmail.com
from __future__ import print_function, unicode_literals
import psycopg2
import requests
import time
import logging
from secret import DATABASES, MAPZEN_KEY

GEOCODE_STATUS_CODES = {
    1: 'Pending',
    2: 'Failed Mapzen',
    3: 'Completed Mapzen',
    4: 'Failed TAMU',
    5: 'Completed TAMU'
}

# Requires a secret.py file in the same directory containing database connection info

connection_database_name = 'DevVoter'
MAPZEN_URL = 'https://search.mapzen.com/v1/search/structured?api_key=' + MAPZEN_KEY

logger = logging.getLogger('geocode_logging')
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(message)s')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('geocode.log')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Connect to database and return a cursor, which can be used for querying
# Database name is the name of the database as specified in the secret.py file
def connect_to_db(connection_database_name):
  try:
    conn = psycopg2.connect(**DATABASES[connection_database_name])
    cur = conn.cursor()
  except Exception as ex:
    print('Connection error with {}: {}'.format(connection_database_name, ex))
  return conn, cur

# fetch address records to query.  Limit = number of rows requested
def get_unmatched_address_records(cur, limit=1):
    query_address = '''
        SELECT
          HOUSEHOLD_ID,
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
          PLACE_NAME,
          STATE_NAME,
          ZIP_CODE
        FROM HOUSEHOLD_DIM
        WHERE GEOCODE_STATUS = 1
        LIMIT {}
        '''.format(limit)

    # Run the address query
    cur.execute(query_address)
    return cur.fetchall()

# Request Mapzen, check status
def request_from_mapzen(addr_row):
    query_string = MAPZEN_URL
    household_id = addr_row[0]
    address_dict = dict(
        # Join all non-empty items for address itself
        address=' '.join([r for r in addr_row[1:11] if r and r is not '']),
        locality=addr_row[11],
        region=addr_row[12],
        postal_code=addr_row[13]
    )

    for k, v in address_dict.items():
        if v is not '':
            query_string += '&{}={}'.format(k, v)

    response = requests.get(query_string)
    # Check headers, status for success and rate limiting
    if response.status_code == 200:
        response_json = response.json()
        if len(response_json['features']) == 0:
            return household_id, None

        feature = response_json['features'][0]
        if not feature['properties']['accuracy'] == 'point':
            return household_id, None

        geom_dict = dict(lon=feature['geometry']['coordinates'][0],
                         lat=feature['geometry']['coordinates'][1])

        return household_id, geom_dict

    # Notifies that you've gone past rate limit, could be per second though
    # TODO: When daemonizing, figure out how to use this
    elif response.status_code == 429:
        time.sleep(0.3)
    else:
        logger.info('ID: {} failed'.format(household_id))
        # Not sure if should just return failed state here
        return household_id, None

# Either update record to coordinates or change status to failed
def update_address_record(cur, household_id, addr_dict):
    if addr_dict:
        status = 3
        update_statement = '''
            UPDATE household_dim
            SET
                GEOM = ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326),
                GEOCODE_STATUS = {g_status}
            WHERE HOUSEHOLD_ID = {h_id}
            '''.format(lon=addr_dict['lon'],
                       lat=addr_dict['lat'],
                       g_status=status,
                       h_id=household_id)
    else:
        status = 2
        update_statement = '''
            UPDATE household_dim
            SET GEOCODE_STATUS = {g_status}
            WHERE HOUSEHOLD_ID = {h_id}
            '''.format(g_status=status, h_id=household_id)

    cur.execute(update_statement)
    logger.info(
        '{h_id} updated with status {g_status}'.format(h_id=household_id, g_status=status)
        )

if __name__ == '__main__':
    # Get the database cursor
    conn, cur = connect_to_db(connection_database_name)
    results = get_unmatched_address_records(cur, limit=5)

    for row in results:
        h_id, geom = request_from_mapzen(row)
        if h_id:
            update_address_record(cur, h_id, geom)
        time.sleep(0.2)
        
    conn.commit()
    cur.close()
    conn.close()

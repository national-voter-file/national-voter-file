# Query from database to get address data

# Contributers: Mike Bruno bruno615@gmail.com
from __future__ import print_function, unicode_literals
import psycopg2
import requests
import os
from datetime import datetime, timedelta
from shapely.geometry import Point, LineString
import time


GEOCODE_STATUS_CODES = {
    1: 'Pending',
    2: 'Failed Mapzen',
    3: 'Completed Mapzen',
    4: 'Failed TAMU',
    5: 'Completed TAMU'
}

# Requires a secret.py file in the same directory containing database connection info

# connection_database_name = 'DevVoter'
ES_URL = 'http://elasticsearc/census/_search'

POINT_QUERY = {
  'query': {
    'bool': {
      'must': [
        {'term': {'properties.number': ''}},
        {'term': {'properties.state': ''}}
      ],
      'should': [
        {'term': {"properties.zip": ''}},
        {'term': {"properties.city": ''}},
        {'term': {"properties.street": ''}}
      ]
    }
  }
}

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

def handle_census_range(range_from, range_to):
    if range_from:
        from_int = int(range_from) if range_from.isdigit() else 0
    if range_to:
        to_int = int(range_to) if range_to.isdigit() else 0

    range_even = from_int % 2 == 0 and to_int % 2 == 0

    if from_int > to_int:
        range_diff = from_int - to_int
    else:
        range_diff = to_int - from_int

    return {
        'is_even': range_even,
        'from_int': from_int,
        'to_int': to_int,
        'range_diff': range_diff
    }

def interpolate_census(data, res_data):
    tiger_feat = res_data['_source']
    data_line = LineString([Point(*p) for p in tiger_feat['geometry']])
    line_dist = data_line.distance

    addr_int = int(data['ADDRESS_NUMBER'])
    addr_is_even = addr_int % 2 == 0

    l_range = handle_census_range(tiger_feat['properties']['LFROMHN'],
                                  tiger_feat['properties']['LTOHN'])
    r_range = handle_census_range(tiger_feat['properties']['RFROMHN'],
                                  tiger_feat['properties']['RTOHN'])

# Request Mapzen, check status
def request_from_mapzen(addr_row, mapzen_url):
    query_string = mapzen_url
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

    elif response.status_code == 429:
        time.sleep(0.3)
    else:
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

def run_geocoder(config, log):
    # Check env var set to postpone execution if hitting rate limits
    wait_time = os.environ.get('WAIT_TIME', None)
    if wait_time:
        wait_dt = datetime.strptime(wait_time, '%Y-%m-%d %H:%M')
        if wait_dt > datetime.now():
            return

    try:
        conn = psycopg2.connect(**config['databases']['DevVoter'])
        cur = conn.cursor()
    except Exception as ex:
        log.error('Connection error with {}: {}'.format(config['databases']['DevVoter']['database'], ex))

    results = get_unmatched_address_records(cur, limit=5)

    mapzen_url = MAPZEN_URL + config['mapzen']['apikey']
    for row in results:
        h_id, geom = request_from_mapzen(row, mapzen_url)
        if h_id:
            update_address_record(cur, h_id, geom)
            log.info('Updated address with household id: {}'.format(h_id))
        else:
            time_wait = (datetime.now() + timedelta(hours=1))
            os.environ['WAIT_TIME'] = time_wait.strftime('%Y-%m-%d %H:%M')
            log.info('Setting wait time to {}'.format(os.environ['WAIT_TIME']))
            break
        time.sleep(0.2)

    conn.commit()
    cur.close()
    conn.close()

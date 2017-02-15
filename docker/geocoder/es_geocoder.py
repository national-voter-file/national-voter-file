# Query from database to get address data

from __future__ import print_function, unicode_literals
import psycopg2
import requests
import os
from datetime import datetime, timedelta
from shapely.geometry import Point, LineString
import aiohttp
import asyncio
import aiopg
import json
import time


GEOCODE_STATUS_CODES = {
    1: 'Pending',
    2: 'Failed Mapzen',
    3: 'Completed Mapzen',
    4: 'Failed TAMU',
    5: 'Completed TAMU'
}

SQL_COLUMNS = [
    'HOUSEHOLD_ID',
    'ADDRESS_NUMBER',
    'STREET_NAME',
    'STREET_NAME_POST_TYPE',
    'PLACE_NAME',
    'STATE_NAME',
    'ZIP_CODE'
]

# connection_database_name = 'DevVoter'
ES_URL = 'http://elasticsearch:9200/{q_idx}/_search'


async def create_point_query(data):
    point_query = {
        'query': {
            'bool': {
                'must': [
                    {'term': {'properties.number': data['ADDRESS_NUMBER']}},
                    {'term': {'properties.state': data['STATE_NAME'].lower()}}
                ],
                'should': [
                    {'term': {'properties.zip': str(data['ZIP_CODE'])}},
                    {'term': {'properties.city': data['PLACE_NAME'].lower()}}
                ]
            }
        }
    }
    if data['STREET_NAME_POST_TYPE']:
        point_query['query']['bool']['should'].append(
            {'term': {'properties.street': data['STREET_NAME_POST_TYPE'].lower()}}
        )

    for s in data['STREET_NAME'].split(' '):
        point_query['query']['bool']['must'].append(
            {'term': {"properties.street": s.lower()}}
        )

    return point_query


async def create_census_query(data):
    census_query = {
        'query': {
            'bool': {
                'must': [
                    {'term': {'properties.STATE': data['STATE_NAME'].lower()}}
                ],
                'should': [
                    {'term': {'properties.ZIPL': str(data['ZIP_CODE'])}},
                    {'term': {'properties.ZIPR': str(data['ZIP_CODE'])}}
                ],
                'filter': {
                    'bool': {
                        'should': [
                            {
                                'bool': {
                                    'must': [
                                        {
                                            'bool': {
                                                'should': [
                                                    {'range': {'properties.LFROMHN': {'lte': data['ADDRESS_NUMBER']}}},
                                                    {'range': {'properties.RFROMHN': {'lte': data['ADDRESS_NUMBER']}}}
                                                ]
                                            }
                                        },
                                        {
                                            'bool': {
                                                'should': [
                                                    {'range': {'properties.LTOHN': {'gte': data['ADDRESS_NUMBER']}}},
                                                    {'range': {'properties.RTOHN': {'gte': data['ADDRESS_NUMBER']}}}
                                                ]
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                'bool': {
                                    'must': [
                                        {
                                            'bool': {
                                                'should': [
                                                    {'range': {'properties.LFROMHN': {'gte': data['ADDRESS_NUMBER']}}},
                                                    {'range': {'properties.RFROMHN': {'gte': data['ADDRESS_NUMBER']}}}
                                                ]
                                            }
                                        },
                                        {
                                            'bool': {
                                                'should': [
                                                    {'range': {'properties.LTOHN': {'lte': data['ADDRESS_NUMBER']}}},
                                                    {'range': {'properties.RTOHN': {'lte': data['ADDRESS_NUMBER']}}}
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
    }
    if data['STREET_NAME_POST_TYPE']:
        census_query['query']['bool']['should'].append(
            {'term': {'properties.FULLNAME': data['STREET_NAME_POST_TYPE'].lower()}}
        )

    for s in data['STREET_NAME'].split(' '):
        census_query['query']['bool']['must'].append(
            {'term': {"properties.FULLNAME": s.lower()}}
        )
    return census_query


async def handle_census_range(range_from, range_to):
    from_int = 0
    to_int = 0
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


async def interpolate_census(data, res_data):
    tiger_feat = res_data['_source']
    data_line = LineString([Point(*p) for p in tiger_feat['geometry']['coordinates']])
    line_len = data_line.length

    addr_int = int(data['ADDRESS_NUMBER'])
    addr_is_even = addr_int % 2 == 0

    l_range = await handle_census_range(tiger_feat['properties']['LFROMHN'],
                                        tiger_feat['properties']['LTOHN'])
    r_range = await handle_census_range(tiger_feat['properties']['RFROMHN'],
                                        tiger_feat['properties']['RTOHN'])

    if addr_is_even == l_range['is_even']:
        tiger_range = l_range
    elif addr_is_even == r_range['is_even']:
        tiger_range = r_range
    else:
        # TODO: Throw error, for now default to l_range
        tiger_range = l_range

    # Check for divide by zero errors, otherwise create distance
    if tiger_range['range_diff'] == 0:
        range_dist = 0
    elif tiger_range['from_int'] > tiger_range['to_int']:
        range_dist = ((tiger_range['from_int'] - addr_int) / tiger_range['range_diff']) * line_len
    else:
        range_dist = ((addr_int - tiger_range['from_int']) / tiger_range['range_diff']) * line_len

    inter_pt = data_line.interpolate(range_dist)
    return {'lat': inter_pt.y, 'lon': inter_pt.x}


async def request_elasticsearch(client, addr_row, q_type='census'):
    if q_type == 'census':
        query_data = await create_census_query(addr_row)
    elif q_type == 'address':
        query_data = await create_point_query(addr_row)

    async with client.post(ES_URL.format(q_idx=q_type),
                           data=json.dumps(query_data)) as response:
        response_json = await response.json()

        if not 'hits' in response_json:
            return addr_row['HOUSEHOLD_ID'], None
        elif response_json['hits'].get('hits', 0) == 0:
            return addr_row['HOUSEHOLD_ID'], None
        elif len(response_json['hits']['hits']) == 0:
            return addr_row['HOUSEHOLD_ID'], None

        addr_hit = response_json['hits']['hits'][0]
        if q_type == 'address':
            geom_dict = dict(lon=addr_hit['geometry']['coordinates'][0],
                             lat=addr_hit['geometry']['coordinates'][1])
        elif q_type == 'census':
            geom_dict = await interpolate_census(addr_row, addr_hit)

        return addr_row['HOUSEHOLD_ID'], geom_dict


async def get_unmatched_addresses(db_conn):
    async with aiopg.create_pool(db_conn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                query_address = '''
                    SELECT {columns}
                    FROM HOUSEHOLD_DIM
                    WHERE GEOCODE_STATUS = 1
                    LIMIT {limit}
                    '''.format(columns=', '.join(SQL_COLUMNS), limit=1000)
                await cur.execute(query_address)
                return await cur.fetchall()


async def update_address(db_conn, household_id, addr_dict):
    async with aiopg.create_pool(db_conn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
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
                await cur.execute(update_statement)


async def handle_update(client, db_conn, log, row):
    h_id, geom = await request_elasticsearch(client, row, q_type='census')
    if h_id:
        await update_address(db_conn, h_id, geom)
        log.info('Updated address with household id: {}'.format(h_id))


async def geocoder_loop(config, log, db_conn, loop, client):
    addrs_to_geocode = await get_unmatched_addresses(db_conn)
    addr_dicts = [dict(zip(SQL_COLUMNS, a)) for a in addrs_to_geocode]

    await asyncio.gather(*[handle_update(client, db_conn, log, row)
                           for row in addr_dicts])


def run_geocoder(config, log):
    dev_config = config['databases']['DevVoter']
    DB_CONN = 'dbname={} user={} password={} host={}'.format(
        dev_config['database'],
        dev_config['user'],
        dev_config['password'],
        dev_config['host']
    )

    loop = asyncio.get_event_loop()
    # TODO: Working on getting the right number of concurrent connections
    conn = aiohttp.TCPConnector(limit=20)
    client = aiohttp.ClientSession(connector=conn, loop=loop)
    loop.run_until_complete(geocoder_loop(config, log, DB_CONN, loop, client))

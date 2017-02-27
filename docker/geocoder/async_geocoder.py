import os
import aiohttp
import asyncio
import asyncpg
import json


GEOCODE_STATUS_CODES = {
    1: 'Pending',
    2: 'Failed Mapzen',
    3: 'Completed Mapzen',
    4: 'Failed TAMU',
    5: 'Completed TAMU'
}


class AsyncGeocoder(object):
    """
    This is the base class for asynchronously geocoding and loading the data
    into a database configured through the docker-compose setup. To run the
    geocoder, just create an instance and then call run().

    The main item that needs to be changed in subclasses is the conn_limit
    property which controls the max amount of HTTP requests at one time. For any
    rate-limited APIs, you'll need to lower it significantly.

    Any properties can be overriden with kwargs, and in any subclass you'll have
    to implement the request_geocoder method returning an awaited tuple with
    household_id and a dictionary structured as GeoJSON geometry.
    """

    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': None,
        'user': 'postgres',
        'password': None
    }

    sql_cols = [
        'HOUSEHOLD_ID',
        'ADDRESS_NUMBER',
        'STREET_NAME',
        'STREET_NAME_POST_TYPE',
        'PLACE_NAME',
        'STATE_NAME',
        'ZIP_CODE'
    ]

    sem_count = 1000
    conn_limit = 200
    query_limit = 10000
    log = None

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def run(self):
        sem = asyncio.Semaphore(self.sem_count)
        loop = asyncio.get_event_loop()
        conn = aiohttp.TCPConnector(limit=self.conn_limit)
        client = aiohttp.ClientSession(connector=conn, loop=loop)
        loop.run_until_complete(self.geocoder_loop(sem, client))

    async def geocoder_loop(self, sem, client):
        """
        Indefinitely loops through the geocoder coroutine, continuing to query
        the database, geocode rows, and update the database with returned values.
        """
        pool = await asyncpg.create_pool(**self.db_config)
        async with sem:
            while True:
                addrs_to_geocode = await self.get_unmatched_addresses(pool)
                await asyncio.gather(*[self.handle_update(client, pool, row)
                                       for row in addrs_to_geocode])

    async def get_unmatched_addresses(self, pool):
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Run the query passing the request argument.
                query_address = '''
                    SELECT {columns}
                    FROM HOUSEHOLD_DIM
                    WHERE GEOCODE_STATUS = 1
                    LIMIT {limit}
                    '''.format(columns=', '.join(self.sql_cols),
                               limit=self.query_limit)
                return await conn.fetch(query_address)


    async def update_address(self, pool, household_id, addr_dict):
        async with pool.acquire() as conn:
            async with conn.transaction():
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
                await conn.execute(update_statement)


    async def handle_update(self, client, pool, row):
        h_id, geom = await self.request_geocoder(client, row)
        if h_id:
            await self.update_address(pool, h_id, geom)
            self.log.info('Updated address with household id: {}'.format(h_id))

    async def request_geocoder(self, client, row):
        """
        Main method that needs to be implemented in subclasses, asynchronously
        requesting a geocoder service.

        Inputs:
            - client: aiohttp.ClientSession for event loop
            - row: Dictionary-like object with the input address data
        """
        raise NotImplementedError('Must implement request_geocoder method')

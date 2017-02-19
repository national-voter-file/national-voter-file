from async_geocoder import AsyncGeocoder
from shapely.geometry import Point, LineString
import json


class ElasticGeocoder(AsyncGeocoder):
    """
    Implements AsyncGeocoder with an Elasticsearch instance managed through a
    docker-compose container. Works for two types of geocoding from the CFPB
    grasshopper-loader repo, address point and TIGER ADDRFEAT data. Interpolates
    any ADDRFEAT address range in order to get a single point.
    """
    q_type = 'census'
    es_url = 'http://elasticsearch:9200/{}/_search'.format(q_type)

    def __init__(self, *args, **kwargs):
        super(ElasticGeocoder, self).__init__(self, *args, **kwargs)

    async def request_geocoder(self, client, row):
        if self.q_type == 'census':
            query_data = await self.create_census_query(row)
        elif self.q_type == 'address':
            query_data = await self.create_point_query(row)

        async with client.post(self.es_url.format(q_idx=self.q_type),
                               data=json.dumps(query_data)) as response:
            response_json = await response.json()

            if not 'hits' in response_json:
                return row['household_id'], None
            elif response_json['hits'].get('hits', 0) == 0:
                return row['household_id'], None
            elif len(response_json['hits']['hits']) == 0:
                return row['household_id'], None

            addr_hit = response_json['hits']['hits'][0]
            if self.q_type == 'address':
                geom_dict = dict(lon=addr_hit['geometry']['coordinates'][0],
                                 lat=addr_hit['geometry']['coordinates'][1])
            elif self.q_type == 'census':
                geom_dict = await self.interpolate_census(row, addr_hit)

            return row['household_id'], geom_dict

    async def handle_census_range(self, range_from, range_to):
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

    async def interpolate_census(self, data, res_data):
        tiger_feat = res_data['_source']
        data_line = LineString(
            [Point(*p) for p in tiger_feat['geometry']['coordinates']]
        )
        line_len = data_line.length

        addr_int = int(data['address_number'])
        addr_is_even = addr_int % 2 == 0

        l_range = await self.handle_census_range(
            tiger_feat['properties']['LFROMHN'],
            tiger_feat['properties']['LTOHN']
        )
        r_range = await self.handle_census_range(
            tiger_feat['properties']['RFROMHN'],
            tiger_feat['properties']['RTOHN']
        )

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
            range_dist = ((tiger_range['from_int'] - addr_int) /
                           tiger_range['range_diff']) * line_len
        else:
            range_dist = ((addr_int - tiger_range['from_int']) /
                           tiger_range['range_diff']) * line_len

        inter_pt = data_line.interpolate(range_dist)

        return {'lat': inter_pt.y, 'lon': inter_pt.x}

    async def create_point_query(self, data):
        point_query = {
            'query': {
                'bool': {
                    'must': [
                        {'term': {'properties.number': data['address_number']}},
                        {'term': {'properties.state': data['state_name'].lower()}}
                    ],
                    'should': [
                        {'term': {'properties.zip': str(data['zip_code'])}},
                        {'term': {'properties.city': data['place_name'].lower()}}
                    ]
                }
            }
        }
        if data['street_name_post_type']:
            point_query['query']['bool']['should'].append(
                {'term': {'properties.street': data['street_name_post_type'].lower()}}
            )

        for s in data['street_name'].split(' '):
            point_query['query']['bool']['must'].append(
                {'term': {"properties.street": s.lower()}}
            )

        return point_query

    async def create_census_query(self, data):
        census_query = {
            'query': {
                'bool': {
                    'must': [
                        {'term': {'properties.STATE': data['state_name'].lower()}}
                    ],
                    'should': [
                        {'term': {'properties.ZIPL': str(data['zip_code'])}},
                        {'term': {'properties.ZIPR': str(data['zip_code'])}}
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
                                                        {'range': {'properties.LFROMHN': {'lte': data['address_number']}}},
                                                        {'range': {'properties.RFROMHN': {'lte': data['address_number']}}}
                                                    ]
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'should': [
                                                        {'range': {'properties.LTOHN': {'gte': data['address_number']}}},
                                                        {'range': {'properties.RTOHN': {'gte': data['address_number']}}}
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
                                                        {'range': {'properties.LFROMHN': {'gte': data['address_number']}}},
                                                        {'range': {'properties.RFROMHN': {'gte': data['address_number']}}}
                                                    ]
                                                }
                                            },
                                            {
                                                'bool': {
                                                    'should': [
                                                        {'range': {'properties.LTOHN': {'lte': data['address_number']}}},
                                                        {'range': {'properties.RTOHN': {'lte': data['address_number']}}}
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
        if data['street_name_post_type']:
            census_query['query']['bool']['should'].append(
                {'term': {'properties.FULLNAME': data['street_name_post_type'].lower()}}
            )

        for s in data['street_name'].split(' '):
            census_query['query']['bool']['must'].append(
                {'term': {"properties.FULLNAME": s.lower()}}
            )
        return census_query


def run_geocoder(config, log):
    es_geocoder = ElasticGeocoder(
        db_config=config['databases']['DevVoter'],
        q_type='census',
        es_url='http://elasticsearch:9200/{}/_search'.format('census'),
        log=log
    )
    es_geocoder.run()

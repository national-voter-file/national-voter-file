import requests
import pandas as pd
import csv
import sys
import re
import collections
from jsonmerge import merge

API_URL="http://api.censusreporter.org/1.0/data/show/{release}?table_ids={table_ids}&geo_ids={geoids}"


def _clean_list_arg(arg,default):
    if arg is None:
        arg = default
    if isinstance(arg,str):
        arg = [arg]
    return arg

def get_url_response(tables, geoids, release):
    url = API_URL.format(table_ids=','.join(tables).upper(),
                         geoids=','.join(geoids),
                         release=release)

    response = requests.get(url)
    return response.json()


def json_data(tables=None, geoids=None, release='latest'):
    """Make a basic API request for data for a given table, geoid, and/or release.
    tables -- An ACS table ID as a string, or a list of such IDs. Default: 'B01001'
    geoids -- A Census geoID as a string, or a list of such IDs. Default: '040|01000US' ('all states in the US')
    release -- The ACS release from which to retrieve data. Should be one of:
        latest - (default) the ACS release which has data for all of the requested geographies
        acs2013_1yr - the 2013 1-year ACS data. Only includes geographies with population >65,000
        acs2013_3yr - the 2011-13 3-year ACS data. Only includes geographies with population >20,000
        acs2013_5yr - the 2009-13 5-year ACS data. Includes all geographies covered in the ACS.
    """
    geoids = _clean_list_arg(geoids,'040|01000US')
    tables = _clean_list_arg(tables,'B01001')

    #If the URL is too big it will fail, estimating the size here and if it is too big we'll break this up
    #This should never happen, but we're going to check just to make sure
    maxURLSize = 4020
    geoSize = len(geoids[0]) + 1
    tblSize = len(tables[0]) + 1
    urlSize = (len(tables) * tblSize) + (len(geoids) * geoSize)

    if urlSize > maxURLSize:
        tableSize = len(tables) * tblSize
        maxGeos = int((maxURLSize - tableSize) / geoSize)
        print("URL maybe too big, breaking up.")
        print((len(tables) * tblSize) + (len(geoids[:maxGeos]) * geoSize))
        resp = get_url_response(tables, geoids[:maxGeos], release)
        if "error" in resp:
            raise Exception(resp['error'])

        return merge(resp, json_data(tables, geoids[maxGeos:], release))

    response = get_url_response(tables, geoids, release)

    if "error" in response and "release doesn't include GeoID(s) " in response['error']:
        geoList = re.findall(r'(\d+US\w+)\W/', response['error'])

        geoids = [x for x in geoids if x not in geoList]
        if len(geoids) == 0:
            return None

        response = get_url_response(tables, geoids, release)

    return response


def get_dataframe(tables=None, geoids=None, release='latest',level=None,place_names=True,column_names=True):
    """Return a pandas DataFrame object for the given tables and geoids.
    Keyword arguments (all optional):
    tables -- An ACS table ID as a string, or a list of such IDs. Default: 'B01001'
    geoids -- A Census geoID as a string, or a list of such IDs. Default: '040|01000US' ('all states in the US')
    release -- The ACS release from which to retrieve data. Should be one of:
        latest - (default) the ACS release which has data for all of the requested geographies
        acs2013_1yr - the 2013 1-year ACS data. Only includes geographies with population >65,000
        acs2013_3yr - the 2011-13 3-year ACS data. Only includes geographies with population >20,000
        acs2013_5yr - the 2009-13 5-year ACS data. Includes all geographies covered in the ACS.
    level -- if provided, should be an integer representing the maximum "indent level" of columns to be returned. Generally, '0' is the total column.
    place_names -- specify False to omit a 'name' column for each geography row
    column_names -- specify False to preserve the coded column names instead of using verbal labels
    """

    response = json_data(tables, geoids, release)

    if 'error' in response:
        raise Exception(response['error'])

    result_list = []
    for geoid, tables in response['data'].items():
        result = {
            'GEOID': geoid
        }
        for table, table_data in tables.items():
            result.update(table_data['estimate'])

        result_list.append(result)

    df = pd.DataFrame(result_list)
    return df


# Create string translation tables
allowed = ' _01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
delchars = ""
for i in range(255):
	if chr(i) not in allowed: delchars = delchars + str(chr(i))
deltable = str.maketrans(' ','_', delchars)

def fixColName(col):
    # Format column name to remove unwanted chars
    col = str.strip(col)
    col = col.translate(deltable)
    fmtcol = col
    fmtcol = col.lower()

    return fmtcol

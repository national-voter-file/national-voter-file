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
    #Each table uses 7 characters and each geoid uses 13 characters
    maxURLSize = 4020
    urlSize = (len(tables) * 7) + (len(geoids) * 13)

    if urlSize > maxURLSize:
        tableSize = len(tables) * 7
        maxGeos = int((maxURLSize - tableSize) / 13)

        resp = get_url_response(tables, geoids[:maxGeos], release)
        if "error" in resp:
            raise Exception(resp['error'])

        return merge(resp, json_data(tables, geoids[maxGeos:], release))

    return get_url_response(tables, geoids, release)

 


def _prep_data_for_pandas(json_data,include_moe=False):
    """Given a dict of dicts as they come from a Census Reporter API call, set it up to be amenable to pandas.DataFrame.from_dict"""
    result = {}
    for geoid, tables in json_data['data'].items():
        flat = {}
        for table,values in tables.items():
            for kind, columns in values.items():
                if kind == 'estimate':
                    flat.update(columns)
                elif kind == 'error' and include_moe:
                    renamed = dict((k+"_moe",v) for k,v in columns.items())
                    flat.update(renamed)
        result[geoid] = flat
    return result

def _prep_headers_for_pandas(json_data,separator=":", level=None):
    headers = {}
    for table in json_data['tables']:
        stack = [ None ] * 10 # pretty sure no columns are nested deeper than this.
        for column in sorted(json_data['tables'][table]['columns']):
            col_md = json_data['tables'][table]['columns'][column]
            indent = col_md['indent']
            name = col_md['name'].strip(separator)
            stack[indent] = name
            parts = []
            if indent > 0:
                for i in range(1,indent+1):
                    if stack[i] is not None:
                        parts.append(stack[i].strip(separator))
                name = separator.join(parts)
            if level is None or indent <= level:
                headers[column] = name
    return headers

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
    df = pd.DataFrame.from_dict(_prep_data_for_pandas(response),orient='index')
    df = df.reindex_axis(sorted(df.columns), axis=1)
    if column_names or level is not None:
        headers = _prep_headers_for_pandas(response, level=level)
        if level is not None:
            df = df.select(lambda x: x in headers,axis=1)
        if column_names:
            df = df.rename(columns=headers)
    if place_names:
        name_frame = pd.DataFrame.from_dict(response['geography'],orient='index')
        df.insert(0, 'name', name_frame.name) 
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

# This gets all the census data, can be filted by level and state.
# Should play with all the chunk sizes, to see how that affects speed.  I'm leaving a message in censusreporter_api.py for now that will alert you if the size gets too big and it does a json_merge.  json_merge is slow, we want to avoid those.
import pandas as pd
from .censusreporter_api import *
import os
from io import BytesIO
import io
from zipfile import ZipFile
import requests
import datetime
import re
import argparse
from bs4 import BeautifulSoup

def getTractInfo(url, regex=''):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + '/' + node.get('href') for node in soup.find_all('a', href=re.compile(regex))]



BASE_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
YEAR = datetime.datetime.now().year
GAZ_YEAR_URL = '{}{}_Gazetteer/'.format(BASE_URL, YEAR)

# For easier Windows compatibility
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))),
    'dimensionaldata'
)
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

STATE_LIST = [ 'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','PR']
STATE_CODES = {'AL': '01','AK': '02','AZ': '04','AR': '05','CA': '06','CO': '08','CT': '09','DE': '10','DC': '11','FL': '12','GA': '13','HI': '15','ID': '16','IL': '17','IN': '18','IA': '19','KS': '20','KY': '21','LA': '22','ME': '23','MD': '24','MA': '25','MI': '26','MN': '27','MS': '28','MO': '29','MT': '30','NE': '31','NV': '32','NH': '33','NJ': '34','NM': '35','NY': '36','NC': '37','ND': '38','OH': '39','OK': '40','OR':'41','PA': '42','RI': '44','SC': '45','SD': '46','TN': '47','TX': '48','UT': '49','VT': '50','VA': '51','WA': '53','WV': '54','WI': '55','WY': '56','PR':'72'}
STATE_ABBREVS = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MP': 'Northern Mariana Islands',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NA': 'National',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VI': 'Virgin Islands',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}

DATA_TABLES = ['B01001','B03002','B06008','B23001','B19001','B25009','B25077']

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--states", help="State Abbreviation List, space seperated ie NY AK", nargs="*")
parser.add_argument("-t", "--type", help="ALL|County|Upper|Lower|Congress|City|State|Tract space separated", nargs="*")


def get_combinedData(thePD=None, tables=None):
    geoids = thePD.index.tolist()
    try:
        dFrame = get_dataframe(geoids=geoids, tables=tables)
    except Exception as e: #This should never happen, it's handled in censusreporter_api but just in case...
        handledError = "release doesn't include GeoID(s) "
        errorMsg = str(e)
        print(errorMsg)
        if handledError in errorMsg:
            pattern = re.compile("^\s+|\s*,\s*|\s+$")
            geoList = pattern.split(errorMsg.partition(handledError)[2].replace(".", ""))
            thePD = thePD[~thePD.index.isin(geoList)]

            #If everything was not valid, then we'll just return nothing
            if len(thePD) == 0:
                return None

            return get_combinedData(thePD, tables)
        else:
            raise
    else:
        return dFrame
    return None


def get_zip(file_url):
    url = requests.get(file_url)
    zipfile = ZipFile(BytesIO(url.content), 'r')
    zip_names = zipfile.namelist()
    if len(zip_names) == 1:
        file_name = zip_names.pop()
        extracted_file = zipfile.open(file_name).read()
        return extracted_file


# Util for cleaning up column names of extra whitespace
def strip_colnames(df):
    all_cols = df.columns.values.tolist()
    col_dict = {}
    for col in all_cols:
        col_dict[col] = col.strip()
    return df.rename(columns=col_dict)


# Gets voter_file_id from different jurisdiction types
def parse_voter_file_id(row):
    if str(row['GEOID']).endswith('ZZ'):
        return None
    # If not ZZ, return letter for district (Alaska has lettered districts)
    if not str(row['GEOID'])[-1:].isdigit():
        return str(row['GEOID'])[-1:]

    # Multiplier is 100 for congress, 1000 for all other types
    if row['ENTITYTYPE'] == 'congress':
        state_mult = 100
    else:
        state_mult = 1000

    if all([a.isdigit() for a in str(row['GEOID']) + str(row['STATEFP'])]):
        voter_file_id = int(row['GEOID']) - (int(row['STATEFP']) * state_mult)
    else:
        return '1'

    # Some states with 1 district return 0, return 1 for those
    if voter_file_id > 0:
        return str(voter_file_id)
    else:
        return '1'


def get_census_data(geo_type, geo_url, state_list, fips_func,
                    state_idx=(0, 0),
                    census_tables=DATA_TABLES,
                    find_zz=False,
                    delim='\t',
                    chunk_size=250):
    print("Working " + geo_type)

    if requests.get(geo_url).status_code != 200:
        raise ValueError("{} file not found at URL: {}".format(geo_type, geo_url))

    # Changing source if city URL
    if geo_type != 'City' and geo_type != "Tract":
        csv_file = get_zip(geo_url)
        file_source = io.StringIO(csv_file.decode('cp1252'))
    else:
        file_source = geo_url

    reader = pd.read_csv(file_source,
                         delimiter=delim,
                         iterator=True,
                         chunksize=chunk_size)
    context_df_list = []
    census_df_list = []

    for chunk in reader:
        if geo_type == "Tract":
            chunk.rename(columns={'CODE': 'GEOID'}, inplace=True)
            chunk['USPS'] = state_list[0] #Tracts are passed in one state at a time, but don't have this field
        else:
            chunk = chunk.loc[chunk['USPS'].isin(state_list)]
        if find_zz:
            chunk['GEOID'] = chunk['GEOID'].astype(str)
            chunk = chunk.loc[chunk['GEOID'].str.find('ZZ') == -1]
        if len(chunk) > 0:
            chunk['FIPS'] = chunk['GEOID'].apply(fips_func)
            context_df_list.append(chunk)
            chunk = chunk.set_index('FIPS')
            data = get_combinedData(chunk, tables=census_tables)
            census_df_list.append(data)

    context_df = pd.concat(context_df_list)
    census_df = pd.concat(census_df_list)

    context_df['STATEFP'] = context_df['GEOID'].apply(
        lambda x: str(x)[:state_idx[0]].zfill(state_idx[1])
    )
    context_df['ENTITYTYPE'] = geo_type.lower()

    # Check if no census data returned, then just return context info
    if len(census_df.columns.values.tolist()) == 0:
        return strip_colnames(context_df.set_index('FIPS'))

    census_df = census_df.rename(columns={'GEOID': 'FIPS'})
    census_df = strip_colnames(census_df.set_index('FIPS'))
    context_df = strip_colnames(context_df.set_index('FIPS'))

    data = context_df.join(census_df)

    return data


# State process is different enough to warrant its own function
def get_state(state_list, state_codes, census_tables=DATA_TABLES):
    print("Starting State")

    df = pd.DataFrame()

    cTemp = [] #I know there is a better way, but this works for me
    for state in state_list:
        cTemp.append([state, state_codes[state]])

    c = pd.DataFrame(cTemp, columns=['USPS', 'GEOID'])
    c['FIPS'] = c['GEOID'].apply(lambda x: "04000US" + str(x).zfill(2))

    c = strip_colnames(c.set_index('FIPS'))

    data = get_combinedData(c, tables=census_tables)
    print("data Size: " + str(len(data)))
    df = df.append(data)

    c['STATEFP'] = state_codes[state]
    c['ENTITYTYPE'] = "state"

    df = df.rename(columns={'GEOID': 'FIPS'})
    df = strip_colnames(df.set_index('FIPS'))
    data = c.join(df)

    return data


if __name__ == '__main__':
    args = parser.parse_args()

    print("Writing to "+OUTPUT_DIR)
    if args.states is None:
        state_list = STATE_LIST
    else:
        state_list = [element.upper() for element in args.states]
    if args.type is None:
        types = 'ALL'
    else:
        types = [element.upper() for element in args.type]

    for state in state_list:
        if state not in STATE_CODES:
            raise ValueError("Unknown state: " + state)

    # Verify Gazetteer URL
    while requests.get(GAZ_YEAR_URL).status_code != 200:
        YEAR -= 1
        GAZ_YEAR_URL = '{}{}_Gazetteer/'.format(BASE_URL, YEAR)
        print(GAZ_YEAR_URL)

    FILE_BASE_URL = GAZ_YEAR_URL + str(YEAR) + "_Gaz_"
    output_df = pd.DataFrame()

    if 'ALL' in types or "COUNTY" in types:
        county_df = get_census_data(
            'County',
            FILE_BASE_URL + 'counties_national.zip',
            state_list,
            lambda x: "05000US{0:0=5d}".format(int(x)),
            state_idx=(-3, 2)
        )
        county_df['VOTER_FILE_ID'] = county_df.apply(
            parse_voter_file_id,
            axis=1
        )
        output_df = output_df.append(county_df)

    if 'ALL' in types or "CONGRESS" in types:
        """
        Now we do congressional districts.  These are numbered, so we need to guess
        which one it is.  We'll start with the year and subtract 1789 (first congress)
        and divide by 2 (2 year terms), then we'll add 2 more since they don't really
        end on the right year and we want to make sure we get it right.  Then we'll
        test the URL and keep removing 1 until we find it.
        """
        congress = int((YEAR - 1789) / 2) + 2

        conYearURL = FILE_BASE_URL + str(congress) + "CDs_national.zip"
        while requests.get(conYearURL).status_code != 200:
            if congress < 115: #Using 115 as when I wrote this code that was the current number, so I know that exists
                raise ValueError("Crap, can't find congress file at: " + conYearURL)

            congress -= 1
            conYearURL = FILE_BASE_URL + str(congress) + "CDs_national.zip"

        congress_df = get_census_data(
            'Congress',
            conYearURL,
            state_list,
            lambda x: "50000US" + str(x).zfill(4),
            state_idx=(-2, 2)
        )
        congress_df['VOTER_FILE_ID'] = congress_df.apply(
            parse_voter_file_id,
            axis=1
        )
        congress_df['NAME'] = congress_df['VOTER_FILE_ID'].apply(
            lambda x: 'Congressional District {}'.format(x) if x else None
        )
        output_df = pd.concat([output_df, congress_df])

    if 'ALL' in types or "LOWER" in types:
        state_house_df = get_census_data(
            'Lower House',
            FILE_BASE_URL + "sldl_national.zip",
            state_list,
            lambda x: "62000US" + str(x).zfill(5),
            state_idx=(-3, 2),
            find_zz=True
        )
        state_house_df['VOTER_FILE_ID'] = state_house_df.apply(
            parse_voter_file_id,
            axis=1
        )
        output_df = pd.concat([output_df, state_house_df])

    if 'ALL' in types or "UPPER" in types:
        upper_house_df = get_census_data(
            'Upper House',
            FILE_BASE_URL + "sldu_national.zip",
            state_list,
            lambda x: "61000US" + str(x).zfill(5),
            state_idx=(-3, 2),
            find_zz=True
        )
        upper_house_df['VOTER_FILE_ID'] = upper_house_df.apply(
            parse_voter_file_id,
            axis=1
        )
        output_df = pd.concat([output_df, upper_house_df])

    # School Districts: high school pattern is: 96000US0400450,
    # elementary school district pattern is: 95000US0400005

    if 'ALL' in types or "CITY" in types:
        city_base_url = GAZ_YEAR_URL + str(YEAR)
        city_df_list = []
        """
        Instead of building iteration in to the city function, iterate through,
        supplying each base URL, and give each one a state list with only the state
        pulled in the URL
        """
        for state in state_list:
            city_url = '{}_gaz_place_{}.txt'.format(city_base_url, STATE_CODES[state])
            state_city_df = get_census_data(
                'City',
                city_url,
                [state],
                lambda x: "16000US" + str(x).zfill(7),
                state_idx=(-5, 2)
            )
            city_df_list.append(state_city_df)
        city_df = pd.concat(city_df_list)
        output_df = pd.concat([output_df, city_df])

    if 'ALL' in types or "STATE" in types:
        state_df = get_state(state_list, STATE_CODES)
        state_df['NAME'] = state_df['USPS'].apply(lambda x: STATE_ABBREVS[x])
        output_df = pd.concat([output_df, state_df])

    if 'ALL' in types or "TRACT" in types:
        tracts_df_list = []
        div_tract_df_list = []
        temp_tract_df_list = []
        loop = 0
        for state in state_list:
            tract_url = 'http://www2.census.gov/geo/maps/dc10map/tract/st{}_{}'.format(STATE_CODES[state], state.lower())
            if state == 'PR':
                tract_url = tract_url + "_1" #PR Just had to be different
            print(tract_url)

            for division in getTractInfo(tract_url, "^[^#\.]+_"):
                for tract_file in getTractInfo(division[:-1], "\.txt$"): #Just in case there are more than one
                    print(tract_file)
                    if "SP_" not in tract_file: #Some have Spanish langage copies, we don't need that
                      temp_tract_df = get_census_data(
                         'Tract',
                         tract_file,
                         [state],
                         lambda x: "14000US" + str(x).zfill(11),
                         state_idx=(-9, 2),
                         delim=';',
                         chunk_size=200
                      )
                      temp_tract_df_list.append(temp_tract_df)
                div_tract_df_list.append(pd.concat(temp_tract_df_list))
            tracts_df_list.append(pd.concat(div_tract_df_list))

        tract_df = pd.concat(tracts_df_list)
        output_df = pd.concat([output_df, tract_df])
    df_cols = output_df.columns.values.tolist()
    for c in ['FUNCSTAT', 'LSAD', 'SHEETS', 'TYPE']:
        if c in df_cols:
            output_df.drop(c, axis=1, inplace=True)
    output_df.to_csv(os.path.join(OUTPUT_DIR, "census.csv"), index_label="FIPS", sep=',')

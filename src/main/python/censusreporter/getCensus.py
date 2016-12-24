#This gets all the census data, can be filted by level and state.
#Should play with all the chunk sizes, to see how that affects speed.  I'm leaving a message in censusreporter_api.py for now that will alert you if the size gets too big and it does a json_merge.  json_merge is slow, we want to avoid those.
import pandas as pd
from censusreporter_api import *
import os
from io import BytesIO
import io
from zipfile import ZipFile
import requests
import datetime
import re
import argparse


BASE_URL = "http://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
YEAR = datetime.datetime.now().year
GAZ_YEAR_URL = '{}{}_Gazetteer/'.format(BASE_URL, YEAR)

# For easier Windows compatibility
OUTPUT_DIR = os.path.join(
	os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))),
	'dimensionaldata'
)
if not os.path.exists(OUTPUT_DIR):
	os.makedirs(OUTPUT_DIR)

STATE_LIST = [ 'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','PR']
STATE_CODES = {'AL': '01','AK': '02','AZ': '04','AR': '05','CA': '06','CO': '08','CT': '09','DE': '10','DC': '11','FL': '12','GA': '13','HI': '15','ID': '16','IL': '17','IN': '18','IA': '19','KS': '20','KY': '21','LA': '22','ME': '23','MD': '24','MA': '25','MI': '26','MN': '27','MS': '28','MO': '29','MT': '30','NE': '31','NV': '32','NH': '33','NJ': '34','NM': '35','NY': '36','NC': '37','ND': '38','OH': '39','OK': '40','OR':'41','PA': '42','RI': '44','SC': '45','SD': '46','TN': '47','TX': '48','UT': '49','VT': '50','VA': '51','WA': '53','WV': '54','WI': '55','WY': '56','PR':'72'}

DATA_TABLES = ['B01001','B03002','B06008','B23001','B19001','B25009','B25077']

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--states", help="State Abbreviation List, space seperated ie NY AK", nargs="*")
parser.add_argument("-t", "--type", help="ALL|County|Upper|Lower|Congress|City|State space separated", nargs="*")


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
			thePD = thePD[-thePD.index.isin(geoList)]

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


def get_census_data(geo_type, geo_url, state_list, fips_func,
					state_idx=(0, 0),
					census_tables=DATA_TABLES,
					find_zz=False):
	print("Starting " + geo_type)

	if requests.get(geo_url).status_code != 200:
		raise ValueError("{} file not found at URL: {}".format(geo_type, geo_url))

	# Changing source if city URL
	if geo_type != 'City':
		csv_file = get_zip(geo_url)
		file_source = io.StringIO(csv_file.decode('cp1252'))
	else:
		file_source = geo_url

	reader = pd.read_csv(file_source,
						 delimiter='\t',
						 iterator=True,
						 chunksize=250)
	context_df_list = []
	census_df_list = []

	for chunk in reader:
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

	FILE_BASE_URL = GAZ_YEAR_URL + str(YEAR) + "_Gaz_"
	output_df = pd.DataFrame()

	if types == 'ALL' or "COUNTY" in types:
		county_df = get_census_data(
			'County',
			FILE_BASE_URL + 'counties_national.zip',
			state_list,
			lambda x: "05000US{0:0=5d}".format(int(x)),
			state_idx=(-3, 2)
		)
		output_df = output_df.append(county_df)

	if types == 'ALL' or "CONGRESS" in types:
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
		output_df = pd.concat([output_df, congress_df])

	if types == 'ALL' or "LOWER" in types:
		state_house_df = get_census_data(
			'Lower House',
			FILE_BASE_URL + "sldl_national.zip",
			state_list,
			lambda x: "62000US" + str(x).zfill(5),
			state_idx=(-3, 2),
			find_zz=True
		)
		output_df = pd.concat([output_df, state_house_df])

	if types == 'ALL' or "UPPER" in types:
		upper_house_df = get_census_data(
			'Upper House',
			FILE_BASE_URL + "sldu_national.zip",
			state_list,
			lambda x: "61000US" + str(x).zfill(5),
			state_idx=(-3, 2),
			find_zz=True
		)
		output_df = pd.concat([output_df, upper_house_df])

	# School Districts: high school pattern is: 96000US0400450,
	# elementary school district pattern is: 95000US0400005

	if types == 'ALL' or "CITY" in types:
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

	if types == 'ALL' or "STATE" in types:
		state_df = get_state(state_list, STATE_CODES)
		output_df = pd.concat([output_df, state_df])

	output_df.to_csv(os.path.join(OUTPUT_DIR, "census.csv"), index_label="FIPS")

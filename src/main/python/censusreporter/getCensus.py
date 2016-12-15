#This gets all the census data, can be filted by level and state.
#Should play with all the chunk sizes, to see how that affects speed.  I'm leaving a message in censusreporter_api.py for now that will alert you if the size gets too big and it does a json_merge.  json_merge is slow, we want to avoid those.
from pandas import *
from censusreporter_api import *
import os
from io import BytesIO
import io
from zipfile import ZipFile
import requests
import datetime
import re
import argparse

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

#We process this in chunks to limit the number of json merges needed in censusreporter_api
def get_county(countyURL, stateList):
	print("Starting County")

	if requests.get(countyURL).status_code != 200:
		raise ValueError("Counties file not found at URL: " + countyURL)

	csv = get_zip(countyURL)

	reader = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t', iterator=True, chunksize=100)
	df = pd.DataFrame()
	c = pd.DataFrame()
	for chunk in reader:
		chunk = chunk[chunk['USPS'].isin(stateList)]
		if len(chunk) > 0:
			chunk['FIPS']=chunk.apply(lambda row:"05000US{0:0=5d}".format(int(row['GEOID'])),axis=1)
			c = c.append(chunk)
			chunk = chunk.set_index('FIPS')

			data = get_combinedData(chunk, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])
			df = df.append(data)

	c['STATEFP']=c.apply(lambda row:str(row['GEOID'])[:-3].zfill(2),axis=1)
	c['ENTITYFP']=c.apply(lambda row:str(row['GEOID'])[-3:].zfill(3),axis=1)
	c['ENTITYTYPE'] = "county"

	c = c.set_index('FIPS')
	data = pd.concat([c, df],axis=1, join_axes=[c.index])
	return data

def get_congress(congressURL, stateList):
	print("Starting Congress")

	if requests.get(congressURL).status_code != 200:
		raise ValueError("Congress file not found at URL: " + congressURL)

	csv = get_zip(congressURL)

	reader = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t', iterator=True, chunksize=100)
	df = pd.DataFrame()
	c = pd.DataFrame()
	for chunk in reader:
		chunk = chunk[chunk['USPS'].isin(stateList)] #Filter only the states we want
		if len(chunk) > 0:
			chunk['FIPS']=chunk.apply(lambda row:"50000US" + str(row['GEOID']).zfill(4),axis=1)
			c = c.append(chunk)
			chunk = chunk.set_index('FIPS')

			data = get_combinedData(chunk, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])
			df = df.append(data)

	c['STATEFP']=c.apply(lambda row:str(row['GEOID'])[:-2].zfill(2),axis=1)
	c['ENTITYFP']=c.apply(lambda row:str(row['GEOID'])[-2:].zfill(2),axis=1)
	c['ENTITYTYPE'] = "congress"

	c = c.set_index('FIPS')
	data = pd.concat([c, df],axis=1, join_axes=[c.index])
	return data

def get_stateHouse(houseURL, stateList):
	print("Starting State House")

	if requests.get(houseURL).status_code != 200:
		raise ValueError("State House file not found at URL: " + houseURL)

	csv = get_zip(houseURL)

	reader = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t', iterator=True, chunksize=100)
	df = pd.DataFrame()
	c = pd.DataFrame()
	for chunk in reader:
		chunk = chunk[chunk['GEOID'].apply(lambda x: str(x).find("ZZ") == -1)]
		chunk = chunk[chunk['USPS'].isin(stateList)] #Filter only the states we want
		if len(chunk) > 0:
			chunk['FIPS']=chunk.apply(lambda row:"62000US" + str(row['GEOID']).zfill(5),axis=1)
			c = c.append(chunk)
			chunk = chunk.set_index('FIPS')

			data = get_combinedData(chunk, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])
			df = df.append(data)

	c['STATEFP']=c.apply(lambda row:str(row['GEOID'])[:-3].zfill(2),axis=1)
	c['ENTITYFP']=c.apply(lambda row:str(row['GEOID'])[-3:].zfill(3),axis=1)
	c['ENTITYTYPE'] = "lower house"

	c = c.set_index('FIPS')
	data = pd.concat([c, df],axis=1, join_axes=[c.index])
	return data

def get_stateSenate(senateURL, stateList):
	print("Starting State Senate")

	if requests.get(senateURL).status_code != 200:
		raise ValueError("State Senate file not found at URL: " + senateURL)

	csv = get_zip(senateURL)

	reader = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t', iterator=True, chunksize=100)
	df = pd.DataFrame()
	c = pd.DataFrame()
	for chunk in reader:
		chunk = chunk[chunk['GEOID'].apply(lambda x: str(x).find("ZZ") == -1)]
		chunk = chunk[chunk['USPS'].isin(stateList)] #Filter only the states we want
		if len(chunk) > 0:
			chunk['FIPS']=chunk.apply(lambda row:"61000US" + str(row['GEOID']).zfill(5),axis=1)
			c = c.append(chunk)
			chunk = chunk.set_index('FIPS')

			data = get_combinedData(chunk, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])
			print("data Size: " + str(len(data)))
			df = df.append(data)

	c['STATEFP']=c.apply(lambda row:str(row['GEOID'])[:-3].zfill(2),axis=1)
	c['ENTITYFP']=c.apply(lambda row:str(row['GEOID'])[-3:].zfill(3),axis=1)
	c['ENTITYTYPE'] = "upper house"

	c = c.set_index('FIPS')
	data = pd.concat([c, df],axis=1, join_axes=[c.index])
	return data

def get_city(baseURL, stateList, stateCodes):
	print("Starting City")

	data = pd.DataFrame()
	for state in stateList:
		URL = baseURL + "_gaz_place_" + stateCodes[state] + ".txt"
		if requests.get(URL).status_code != 200:
			raise ValueError("City file not found at URL: " + URL)

		reader = pd.read_csv(URL, delimiter='\t', iterator=True, chunksize=100)
		df = pd.DataFrame()
		c = pd.DataFrame()
		for chunk in reader:
			chunk = chunk[chunk['USPS'].isin(stateList)] #Filter only the states we want
			if len(chunk) > 0:
				chunk['FIPS']=chunk.apply(lambda row:"16000US" + str(row['GEOID']).zfill(7),axis=1)
				c = c.append(chunk)
				chunk = chunk.set_index('FIPS')

				data = get_combinedData(chunk, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])
				df = df.append(data)

		c['STATEFP']=c.apply(lambda row:str(row['GEOID'])[:-2].zfill(2),axis=1)
		c['ENTITYFP']=c.apply(lambda row:str(row['GEOID'])[-2:].zfill(5),axis=1) #Check to make sure this is right
		c['ENTITYTYPE'] = "city"

		c = c.set_index('FIPS')
		data.append(pd.concat([c, df],axis=1, join_axes=[c.index]))

	return data

def get_state(stateList, stateCodes):
	print("Starting State")

	df = pd.DataFrame()
	
	cTemp = [] #I know there is a better way, but this works for me
	for state in stateList:
		cTemp.append([state, stateCodes[state]])

	c = pd.DataFrame(cTemp, columns=['USPS', 'GEOID'])
	c['FIPS']=c.apply(lambda row:"04000US" + str(row['GEOID']).zfill(2),axis=1)
	print(c)
	c = c.set_index('FIPS')

	data = get_combinedData(c, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])
	print("data Size: " + str(len(data)))
	df = df.append(data)

	c['STATEFP']=c.apply(lambda row:str(stateCodes[state]),axis=1)
	c['ENTITYFP']=c.apply(lambda row:str(stateCodes[state]),axis=1)
	c['ENTITYTYPE'] = "state"

	data = pd.concat([c, df],axis=1, join_axes=[c.index])

	return data

#########

parser = argparse.ArgumentParser()

parser.add_argument("-s", "--states", help="State Abbreviation List, space seperated ie NY, AK", nargs="*")
parser.add_argument("-t", "--type", help="ALL|County|Upper|Lower|Congress|City|State space seperated", nargs="*")

args = parser.parse_args()
	
if args.states is None:
	stateList = [ 'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','PR']
else:
	stateList = [element.upper() for element in args.states]
if args.type is None:
	types = 'ALL'
else:
	types = [element.upper() for element in args.type]


stateCodes = {'AL': '01','AK': '02','AZ': '04','AR': '05','CA': '06','CO': '08','CT': '09','DE': '10','DC': '11','FL': '12','GA': '13','HI': '15','ID': '16','IL': '17','IN': '18','IA': '19','KS': '20','KY': '21','LA': '22','ME': '23','MD': '24','MA': '25','MI': '26','MN': '27','MS': '28','MO': '29','MT': '30','NE': '31','NV': '32','NH': '33','NJ': '34','NM': '35','NY': '36','NC': '37','ND': '38','OH': '39','OK': '40','OR':'41','PA': '42','RI': '44','SC': '45','SD': '46','TN': '47','TX': '48','UT': '49','VT': '50','VA': '51','WA': '53','WV': '54','WI': '55','WY': '6','PR':'72'}

for state in stateList:
	if state not in stateCodes:
		raise ValueError("Unknown state: " + state)

directory = "../../../../dimensionaldata"
if not os.path.exists(directory):
	os.makedirs(directory)

year = datetime.datetime.now().year

baseURL = "http://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
gazYearURL = baseURL + str(year) + "_Gazetteer/"
while requests.get(gazYearURL).status_code != 200:
	year -= 1
	gazYearURL = baseURL + str(year) + "_Gazetteer/"

fileBaseURL = gazYearURL + str(year) + "_Gaz_"
outData = pd.DataFrame()

if types == 'ALL' or "COUNTY" in types:
	outData = outData.append(get_county(fileBaseURL + "counties_national.zip", stateList))

if types == 'ALL' or "CONGRESS" in types:

#Now we do congressional districts.  These are numbered, so we need to guess which one it is.  We'll start with the year and subtract 1789 (first congress) and divide by 2 (2 year terms), then we'll add 2 more since they don't really end on the right year and we want to make sure we get it right.  Then we'll test the URL and keep removing 1 until we find it.

	congress = int((year - 1789) / 2) + 2

	conYearURL = fileBaseURL + str(congress) + "CDs_national.zip"
	while requests.get(conYearURL).status_code != 200:
		if congress < 115: #Using 115 as when I wrote this code that was the current number, so I know that exists
			raise ValueError("Crap, can't find congress file at: " + conYearURL)

		congress -= 1
		conYearURL = fileBaseURL + str(congress) + "CDs_national.zip"


	outData = outData.append(get_congress(conYearURL, stateList))


if types == 'ALL' or "LOWER" in types:
#Next we'll do lower house (State House)
	outData = outData.append(get_stateHouse(fileBaseURL + "sldl_national.zip", stateList))


if types == 'ALL' or "UPPER" in types:
#Next we'll do upper house (State Senate)
	outData = outData.append(get_stateSenate(fileBaseURL + "sldu_national.zip", stateList))

#School Districts: high school pattern is: 96000US0400450, elementary school district pattern is: 95000US0400005

if "CITY" in types:
#Now we do cities, this one is failing on append inside get_city, no idea why (Something about managers... and we all know managers suck
	outData = outData.append(get_city(gazYearURL + str(year), stateList, stateCodes))

if "STATE" in types:
#Now we do states, this one gets different columns, so the append here fails.  Every column in state exists in outData, but not every column in outData exists in state.  One would think it would just null the missing columns, but apparently not.
	outData = outData.append(get_state(stateList, stateCodes))


outData.to_csv(directory+"/census.csv")

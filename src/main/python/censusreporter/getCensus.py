#This can get really slow, I believe the main issue is jsonmerge in censusreporter_api.py but we need to use that
#I believe it will be faster if we break up the larger data sets in or before get_combinedData and then use pandas.concat
#to recombine.  Will look into that later.

#Another speed up is to look for the "doesn't include" error from censusreporter_api in censusreporter_api instead of here
#Currently Census reporter breaks up large requests into multiples and if it gets that message anywhere it fails back
#Then we restart the whole process after removing the bad geoid.  Problem is that the failure could come after processing
#thousands of valid records, all of which will need to be redone.  Doing it within the census reporter allows us to 
#re-do just that one batch that failed.

#Modify script to put each type of census pull into it's own function, accept a "Type" parameter that would accept
#county, congress, all, etc so script can be used to pull just one type of record or everything as it currently does.
from pandas import *
from censusreporter_api import *
import os
from io import BytesIO
import io
from zipfile import ZipFile
import requests
import datetime
import re

def get_combinedData(thePD=None, tables=None):
	geoids = thePD.index.tolist()
	try:
		dFrame = get_dataframe(geoids=geoids, tables=tables)
	except Exception as e:
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

directory = "../../../../dimensionaldata"
year = datetime.datetime.now().year

baseURL = "http://www2.census.gov/geo/docs/maps-data/data/gazetteer/"
gazYearURL = baseURL + str(year) + "_Gazetteer/"
while requests.get(gazYearURL).status_code != 200:
	year -= 1
	gazYearURL = baseURL + str(year) + "_Gazetteer/"

fileBaseURL = gazYearURL + str(year) + "_Gaz_"

print("Starting County")
countyURL = fileBaseURL + "counties_national.zip"
if requests.get(countyURL).status_code != 200:
	raise ValueError("Counties file not found at URL: " + countyURL)

csv = get_zip(countyURL)

counties = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t')
counties['FIPS']=counties.apply(lambda row:"05000US{0:0=5d}".format(int(row['GEOID'])),axis=1)
c = counties.set_index('FIPS')

df = get_combinedData(c, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])

c['STATEFP']=c.apply(lambda row:str(row['GEOID'])[:-3].zfill(2),axis=1)
c['COUNTYFP']=c.apply(lambda row:str(row['GEOID'])[-3:].zfill(3),axis=1)


data = pd.concat([c, df],axis=1, join_axes=[c.index])

if not os.path.exists(directory):
	os.makedirs(directory)

data.to_csv(directory+"/counties.csv")



#Now we do congressional districts.  These are numbered, so we need to guess which one it is.  We'll start with the year and subtract 1789 (first congress) and divide by 2 (2 year terms), then we'll add 2 more since they don't really end on the right year and we want to make sure we get it right.  Then we'll test the URL and keep removing 1 until we find it.
print("Starting Congress")
congress = int((year - 1789) / 2) + 2

conYearURL = fileBaseURL + str(congress) + "CDs_national.zip"
while requests.get(conYearURL).status_code != 200:
	if congress < 115: #Using 115 as when I wrote this code that was the current number, so I know that exists
		raise ValueError("Crap, can't find congress file at: " + conYearURL)

	congress -= 1
	conYearURL = fileBaseURL + str(congress) + "CDs_national.zip"

csv = get_zip(conYearURL)

cd = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t')
#CD files have some strange GEOIDs that end in ZZ, which are non-censused areas.  Censusreporter says no FIPS code matches, so we're just going to drop those

cd = cd[cd['GEOID'].apply(lambda x: str(x).find("ZZ") == -1)]
cd['FIPS']=cd.apply(lambda row:"50000US" + str(row['GEOID']).zfill(4),axis=1)
cd = cd.set_index('FIPS')

df = get_combinedData(thePD=cd, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])

cd['STATEFP']=cd.apply(lambda row:str(row['GEOID'])[:-2].zfill(2),axis=1)
cd['CongressionalDistFIPS']=cd.apply(lambda row:str(row['GEOID'])[-2:].zfill(2),axis=1)

data = pd.concat([cd, df],axis=1, join_axes=[cd.index])

data.to_csv(directory+"/congress.csv")

#Next we'll do lower house (State House)
print("Starting State House")
csv = get_zip(fileBaseURL + "sldl_national.zip")

sh = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t')
#CD files have some strange GEOIDs that end in ZZ, which are non-censused areas.  Censusreporter says no FIPS code matches, so we're just going to drop those

sh = sh[sh['GEOID'].apply(lambda x: str(x).find("ZZ") == -1)]

sh['FIPS']=sh.apply(lambda row:"62000US" + str(row['GEOID']).zfill(5),axis=1)

sh = sh.set_index('FIPS')

df = get_combinedData(thePD=sh, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])

sh['STATEFP']=sh.apply(lambda row:str(row['GEOID'])[:-3].zfill(2),axis=1)
sh['distFips']=sh.apply(lambda row:str(row['GEOID'])[-3:].zfill(3),axis=1)

data = pd.concat([sh, df],axis=1, join_axes=[sh.index])


data.to_csv(directory+"/stateHouse.csv")

#Next we'll do upper house (State Senate)
print("Starting State Senate")
csv = get_zip(fileBaseURL + "sldu_national.zip")

ss = pd.read_csv(io.StringIO(csv.decode('cp1252')), delimiter='\t')
#CD files have some strange GEOIDs that end in ZZ, which are non-censused areas.  Censusreporter says no FIPS code matches, so we're just going to drop those

ss = ss[ss['GEOID'].apply(lambda x: str(x).find("ZZ") == -1)]
ss['FIPS']=ss.apply(lambda row:"61000US" + str(row['GEOID']).zfill(5),axis=1)
ss = ss.set_index('FIPS')

df = get_combinedData(thePD=ss, tables=['B01001','B01003','B03002','B06008','B23001','B19001','B25009','B25077'])

ss['STATEFP']=ss.apply(lambda row:str(row['GEOID'])[:-3].zfill(2),axis=1)
ss['distFips']=ss.apply(lambda row:str(row['GEOID'])[-3:].zfill(3),axis=1)

data = pd.concat([ss, df],axis=1, join_axes=[ss.index])


data.to_csv(directory+"/stateSenate.csv")

#School Districts: high school pattern is: 96000US0400450, elementary school district pattern is: 95000US0400005

#City/Places pattern is: 16000US4500100


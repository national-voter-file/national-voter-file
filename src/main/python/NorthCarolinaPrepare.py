import usaddress
import csv
import sys
import re
import collections
from datetime import datetime
import datetime as dt
import PrepareUtils

###########################################################
## North Carolina Prepare.py
##
## This script pre-processes the North Carolina voter file to use usaddress module
## to break the address string into standard parts
## Also uses the residential address if no mailing address is provided
##
## Outputs lines that fail the address parser to an error log file
###########################################################
def constructInputFieldList():
	return( [
		'COUNTYCODE',
		'COUNTYNAME',
		'COUNTYVRNUMBER',
		'STATUS',
		'STATUSDESC',
		'REASONCODE',
		'REASONDESC',
		'ABSENTIND', #No idea what this is...
		'NAMEPREFIX',
		'LASTNAME',
		'FIRSTNAME',
		'MIDDLENAME',
		'NAMESUFFIX',
		'RADDNUMBER', #Might need to look at this, it's the full street and address number
#		'RHALFCODE', This doesn't seem to be there
#		'RAPARTMENT', Or this
#		'RPREDIRECTION',
#		'RSTREETNAME',
#		'RPOSTDIRECTION',
		'RCITY',
		'RSTATECD', #The state
		'RZIP5',
#		'RZIP4',
		'MAILADD1',
		'MAILADD2',
		'MAILADD3',
		'MAILADD4',
		'MAILCITY',
		'MAILSTATE',
		'MAILZIP',
		'PHONE',
		'RACE',
		'ETHNIC',
		'ENROLLMENT',
		'GENDER',
		'AGE',
		'BIRTHSTATE',
		'DRIVER_YN', #If they have a license
		'REGDATE',
		'PRECINCT', #Presinct code
		'PRECINTDESC',
		'LD', #Municipality code
		'MUNIDESC',
		'WARDCODE',
		'WARDDESC',
		'CD', #Congressional District
		'SUPERCOURT', #Superior court
		'JUDICIALDIST', #Judicial District
		'SD', #State Senate District
		'AD', #State House District
		'COUNTYCOMM', #County Commissioner Code
		'COUNTYCOMMDESC',
		'TOWNSHIPCODE',
		'TOWNSHIPDESC',
		'SCHOOLDISTCD', #School Dist Code
		'SCHOOLDISTDESC',
		'FIREDISTCD',
		'FIREDISCDESC',
		'WATERDISTCODE',
		'WATERDISTDESC',
		'SEWERDISTCD',
		'SEWERDISTDESC',
		'SANIDISTCD', #Sanitation District Code
		'SANIDISTDESC',
		'RESCUEDISTCD',
		'RESCUEDISTDESC',
		'MUNICDISTCODE', #Not sure...
		'MUNICDISTDESC',
		'DIST1CODE', #Not sure, appears to match Judicial and Super codes, description has something about Prosecutorial so...
		'DIST1DESC',
		'DIST2CODE', #Not sure, blank on all my samples
		'DIST2DESC',
		'CONFIDENTIALIND', #No idea, seems to be a Y/N but N on all my samples
		'AGERANGE', #Age range
		'NCID', #Seems to be a NC ID number
		'VTDCODE', #No idea
		'VITDESC', #No idea, but it's the same as the previous
])

def prepareDate(ncDate):
	return datetime.strptime(ncDate, "%m/%d/%Y").strftime("%Y-%m-%d")

def appendMailingAddress(outrow, row):
	try:
		tagged_address, address_type = usaddress.tag(' '.join([
			row['MAILADD1'],
			row['MAILADD2'],
			row['MAILADD3'],
			row['MAILADD4']]))
	except usaddress.RepeatedLabelError as e :
		print('Warn: Can\'t parse mailing address. Falling back to residential (%s)' % (e.parsed_string))
		tagged_address = {}

	if(len(tagged_address) > 0):
		PrepareUtils.appendMailingAddressFromTaggedFields(outrow, tagged_address, address_type)
	else:
		outrow.update({
			'MAIL_ADDRESS_LINE1':PrepareUtils.constructMailAddr1FromOutRow(outrow),
			'MAIL_ADDRESS_LINE2':PrepareUtils.constructMailAddr2FromOutRow(outrow),
			'MAIL_CITY':outrow['PLACE_NAME'],
			'MAIL_STATE':outrow['STATE_NAME'],
			'MAIL_ZIP_CODE':outrow['ZIP_CODE'],
			'MAIL_COUNTRY':'USA'
			})


def appendJurisdiction(outrow, row):
		outrow.update({
		'COUNTYCODE':row['COUNTYCODE'],
		'PRECINCT':row['PRECINCT'], 
		#'LEGISLATIVE_DIST':row['LD'], 
		#'WARD':row['WARDDESC'],
		'CONGRESSIONAL_DIST':row['CD'],
		'UPPER_HOUSE_DIST':row['SD'],
		'LOWER_HOUSE_DIST':row['AD']})



def constructResidenceAddress(row): #Need question
	aptField = row['RAPARTMENT'].strip();
	return ' '.join([row['RADDNUMBER'],
				row['RHALFCODE'],
				row['RPREDIRECTION'],
				row['RSTREETNAME'],
				row['RPOSTDIRECTION'],
				'Apt '+row['RAPARTMENT'] if aptField and aptField != 'APT' else ''
			])

def constructVoterRegOutrow(row):
	#We don't have birthdate, so we'll guess
	by = int(datetime.now().year) - int(row['AGE'])
	bd = str(dt.date(by, 1, 1))

	return {
		'STATE_VOTER_REF':row['COUNTYVRNUMBER'], #As far as I can tell, these are the same
		'COUNTY_VOTER_REF':row['COUNTYVRNUMBER'],
		'FIRST_NAME':row['FIRSTNAME'],
		'MIDDLE_NAME':row['MIDDLENAME'],
		'LAST_NAME':row['LASTNAME'],
		'NAME_SUFFIX':row['NAMESUFFIX'],
		'BIRTHDATE':bd, 
		'BIRTHDATE_IS_ESTIMATE': 1, #All birthdates are estimates
		'GENDER': row['GENDER'],
		'REGISTRATION_DATE':prepareDate(row['REGDATE']),
		'REGISTRATION_STATUS':row['STATUS'],
		'PARTY':row['ENROLLMENT'], #Do we need to standardize this?
		'PLACE_NAME':row['RCITY'].upper(),
		'STATE_NAME':'NC', #Do we want hard code here?
		'ZIP_CODE':row['RZIP5']
	}

if len(sys.argv) != 2:
	print('Usage: NorthCarolinaPrepare.py <inputfile>');
	quit()


	
inputFile = sys.argv[1]
outputFile = re.sub('\\..*$', '_OUT.csv',inputFile, count=1)
errorFileName = re.sub('\\..*$', '_ERR.csv',inputFile, count=1)
print("Reading from "+inputFile+"-->"+outputFile)
with open(inputFile, encoding='latin-1') as csvfile, \
	open(outputFile, 'w') as outfile, open(errorFileName, 'w') as errorFile:
		#Let's check to see if it has a header, going to read up to 1MB to test
		if (csv.Sniffer().has_header(csvfile.read(2 ** 20))):
			#We do, so reset point to the start, then skip the first line
			csvfile.seek(0)
			next(csvfile)
		else:
			#We don't, so reset back to the start.
			csvfile.seek(0)
		reader = csv.DictReader(csvfile, dialect='excel', fieldnames=constructInputFieldList(), delimiter='\t')

		writer = csv.DictWriter(outfile, fieldnames=PrepareUtils.constructOutputFieldNames())
		writer.writeheader()


		# Create a file for writing addresses that don't parse
		errnames = list(PrepareUtils.constructOutputFieldNames())
		errnames.extend(['PARSED_STRING',"ORIGINAL_TEXT"])
		errWriter = csv.DictWriter(errorFile, fieldnames=errnames)
		errWriter.writeheader()

		i = 0;
		for row in reader:
			# Skip blank lines
			if row['MAILADD1'] is None:
				#if row['STATUS'] != 'A': #This is either an inactive or removed voter, we don't care
				#	print("Skipping bad row for inactive user: " + row['STATUS'])
				#	continue
				for key in row:
					print(key +":"+row[key] if row[key] is not None else 'NONE')
				sys.exit('Bad Row---->')

			outrow = constructVoterRegOutrow(row)

			try:
				#Don't think we need this...
				#addr = constructResidenceAddress(row)
				tagged_address, address_type = usaddress.tag(row['RADDNUMBER']) #This was addr
				PrepareUtils.appendParsedFields(outrow, tagged_address)

				appendMailingAddress(outrow, row)
				appendJurisdiction(outrow, row)

				writer.writerow(outrow)

			### Gets thrown when the US Address parser gets hopelesly confused. Write this out to the errorFile
			### for manual training later
			except usaddress.RepeatedLabelError as e :
				print("\n\n--->Error\n",e.parsed_string, e.original_string)
				outrow.update({
					'PARSED_STRING':e.parsed_string,
					'ORIGINAL_TEXT':e.original_string
				})

				errWriter.writerow(outrow)

			# if i > 1000:
				# break
			# i += 1

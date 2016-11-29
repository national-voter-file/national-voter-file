import usaddress
import csv
import sys
import re
import collections
from datetime import datetime

import PrepareUtils

###########################################################
## New York Prepare.py
##
## This script pre-processes the New York voter file to use usaddress module
## to break the address string into standard parts
## Also uses the residential address if no mailing address is provided
##
## Outputs lines that fail the address parser to an error log file
###########################################################
def constructInputFieldList():
	return( [
		'LASTNAME',
		'FIRSTNAME',
		'MIDDLENAME',
		'NAMESUFFIX',
		'RADDNUMBER',
		'RHALFCODE',
		'RAPARTMENT',
		'RPREDIRECTION',
		'RSTREETNAME',
		'RPOSTDIRECTION',
		'RCITY',
		'RZIP5',
		'RZIP4',
		'MAILADD1',
		'MAILADD2',
		'MAILADD3',
		'MAILADD4',
		'DOB',
		'GENDER',
		'ENROLLMENT',
		'OTHERPARTY',
		'COUNTYCODE',
		'ED',
		'LD',
		'TOWNCITY',
		'WARD',
		'CD',
		'SD',
		'AD',
		'LASTVOTEDDATE',
		'PREVYEARVOTED',
		'PREVCOUNTY',
		'PREVADDRESS',
		'PREVNAME',
		'COUNTYVRNUMBER',
		'REGDATE',
		'VRSOURCE',
		'IDREQUIRED',
		'IDMET',
		'STATUS',
		'REASONCODE',
		'INACT_DATE',
		'PURGE_DATE',
		'SBOEID',
		'VoterHistory'])

def prepareDate(nyDate):
	return datetime.strptime(nyDate, "%Y%m%d").strftime("%Y-%m-%d")

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
		'CONGRESSIONAL_DIST':row['CD'],
		'UPPER_HOUSE_DIST':row['SD'],
		'LOWER_HOUSE_DIST':row['AD']})



def constructResidenceAddress(row):
	aptField = row['RAPARTMENT'].strip();
	return ' '.join([row['RADDNUMBER'],
							 row['RHALFCODE'],
							 row['RPREDIRECTION'],
							 row['RSTREETNAME'],
							 row['RPOSTDIRECTION'],
							 'Apt '+row['RAPARTMENT'] if aptField and aptField != 'APT' else ''
						])

def constructVoterRegOutrow(row):
	return {
		'STATE_VOTER_REF':row['SBOEID'],
		'COUNTY_VOTER_REF':row['COUNTYVRNUMBER'],
		'FIRST_NAME':row['FIRSTNAME'],
		'MIDDLE_NAME':row['MIDDLENAME'],
		'LAST_NAME':row['LASTNAME'],
		'NAME_SUFFIX':row['NAMESUFFIX'],
		'BIRTHDATE':prepareDate(row['DOB']),
		'GENDER': row['GENDER'],
		'REGISTRATION_DATE':prepareDate(row['REGDATE']),
		'REGISTRATION_STATUS':row['STATUS'].strip(),
		'PARTY':(row['ENROLLMENT'] if row['ENROLLMENT'] != 'OTH'  else row['OTHERPARTY']),
		'PLACE_NAME':row['RCITY'].upper(),
		'STATE_NAME':'NY',
		'ZIP_CODE':row['RZIP5']
	}

if len(sys.argv) != 2:
	print('Usage: NewYorkPrepare.py <inputfile>');
	quit()


	
inputFile = sys.argv[1]
outputFile = re.sub('\\..*$', '_OUT.csv',inputFile, count=1)
errorFileName = re.sub('\\..*$', '_ERR.csv',inputFile, count=1)
print("Reading from "+inputFile+"-->"+outputFile)
with open(inputFile, encoding='latin-1') as csvfile, \
	open(outputFile, 'w') as outfile, open(errorFileName, 'w') as errorFile:
		reader = csv.DictReader(csvfile, dialect='excel', fieldnames=constructInputFieldList())

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
			if row['MAILADD4'] is None:
				for key in row:
					print(key +":"+row[key] if row[key] is not None else 'NONE')
				sys.exit('Bad Row---->')

			outrow = constructVoterRegOutrow(row)

			try:
				addr = constructResidenceAddress(row)
				tagged_address, address_type = usaddress.tag(addr)
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



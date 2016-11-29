import usaddress
import csv
import sys
import re
import collections
from datetime import datetime

import PrepareUtils

###########################################################
## Florida Prepare.py
##
## This script pre-processes the New York voter file to use usaddress module
## to break the address string into standard parts
## Also uses the residential address if no mailing address is provided
##
## Outputs lines that fail the address parser to an error log file
###########################################################
def constructInputFieldList():
	return( [
		'County Code',
		'Voter ID',
		'Name Last',
		'Name Suffix',
		'Name First',
		'Name Middle',
		'Requested public records exemption',
		'Residence Address Line 1',
		'Residence Address Line 2',
		'Residence City (USPS)',
		'Residence State',
		'Residence Zipcode',
		'Mailing Address Line 1',
		'Mailing Address Line 2',
		'Mailing Address Line 3',
		'Mailing City',
		'Mailing State',
		'Mailing Zipcode',
		'Mailing Country',
		'Gender',
		'Race',
		'Birth Date',
		'Registration Date',
		'Party Affiliation',
		'Precinct',
		'Precinct Group',
		'Precinct Split',
		'Precinct Suffix',
		'Voter Status',
		'Congressional District',
		'House District',
		'Senate District',
		'County Commission District',
		'School Board District',
		'Daytime Area Code',
		'Daytime Phone Number',
		'Daytime Phone Extension',
		'Email address'])

def prepareDate(voterFileDate):
	return datetime.strptime(voterFileDate, "%m/%d/%Y").strftime("%Y-%m-%d") if voterFileDate else ''

def appendMailingAddress(outrow, row):
	if(row['Mailing Address Line 1'].strip()):
		outrow.update({
			'MAIL_ADDRESS_LINE1':row['Mailing Address Line 1'],
			'MAIL_ADDRESS_LINE2':row['Mailing Address Line 2'],
			# Note: Throwing away mailing address line 3
			'MAIL_CITY':row['Mailing City'],
			'MAIL_STATE':row['Mailing State'],
			'MAIL_ZIP_CODE':row['Mailing Zipcode'],
			'MAIL_COUNTRY':row['Mailing Country']
		})
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

		# Precinct split will be our actual key to the voter's precinct
		# sometimes field is blank so fall back to precinct.
		precinctSplit = row['Precinct Split'].strip() if row['Precinct Split'].strip() else row['Precinct'].strip()
		
		# Get rid of the useless trailing dot
		precinctSplit = re.sub("\.$", "", precinctSplit)


		print("Split[%s] precinct[%s] guess[%s]"%(row['Precinct Split'], row['Precinct'],precinctSplit))
		
		outrow.update({
		'COUNTYCODE':row['County Code'],
		'PRECINCT':row['Precinct'],
		'CONGRESSIONAL_DIST':row['Congressional District'],
		'UPPER_HOUSE_DIST':row['Senate District'],
		'LOWER_HOUSE_DIST':row['House District'],
		'COUNTY_BOARD_DIST':row['County Commission District'],
		'SCHOOL_BOARD_DIST':row['School Board District'],
		'PRECINCT_SPLIT':precinctSplit
		})



def constructResidenceAddress(row):
	return ' '.join([row['Residence Address Line 1'],
							 row['Residence Address Line 2']])

def constructVoterRegOutrow(row):
	phone = row['Daytime Phone Number'].strip()
	
	phoneStr = '(%s) %s-%s'%(row['Daytime Area Code'], phone[:3],phone[3:]) if phone else ''

	return {
		'STATE_VOTER_REF':"FL"+row['Voter ID'],
		'FIRST_NAME':row['Name First'],
		'MIDDLE_NAME':row['Name Middle'],
		'LAST_NAME':row['Name Last'],
		'NAME_SUFFIX':row['Name Suffix'],
		'BIRTHDATE':prepareDate(row['Birth Date']),
		'GENDER': row['Gender'],
		'REGISTRATION_DATE':prepareDate(row['Registration Date']),
		'REGISTRATION_STATUS':row['Voter Status'],
		'PARTY':row['Party Affiliation'],
		'PLACE_NAME':row['Residence City (USPS)'].upper(),
		'STATE_NAME':'FL',
		'ZIP_CODE':row['Residence Zipcode'],
		'EMAIL':row['Email address'],
		'PHONE':phoneStr
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
		reader = csv.DictReader(csvfile, delimiter='\t', dialect='excel', fieldnames=constructInputFieldList())

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
#			if row['MAILADD4'] is None:
#				for key in row:
#					print(key +":"+row[key] if row[key] is not None else 'NONE')
#				sys.exit('Bad Row---->')

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



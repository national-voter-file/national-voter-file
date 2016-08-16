import usaddress
import csv
import sys
import re
import collections

###########################################################
## OhioPrepare.py
##
## This script pre-processes the Ohio voter file to use usaddress module
## to break the address string into standard parts
## Also uses the residential address if no mailing address is provided
## 
## Outputs lines that fail the address parser to an error log file
def appendParsedFields(outrow, addressValues):
	# Construct the output
	outrow.update({
	'ADDRESS_NUMBER': addressValues['AddressNumber'],
	'ADDRESS_NUMBER_PREFIX': addressValues['AddressNumberPrefix'],
	'ADDRESS_NUMBER_SUFFIX': addressValues['AddressNumberSuffix '],
	'BUILDING_NAME': addressValues['BuildingName'],
	'CORNER_OF': addressValues['CornerOf'],
	'INTERSECTION_SEPARATOR': addressValues['IntersectionSeparator'],
	'LANDMARK_NAME': addressValues['LandmarkName'],
	'NOT_ADDRESS': addressValues['NotAddress'],
	'OCCUPANCY_TYPE': addressValues['OccupancyType'],
	'OCCUPANCY_IDENTIFIER': addressValues['OccupancyIdentifier'],
	'PLACE_NAME': addressValues['PlaceName'],
	'STATE_NAME': addressValues['StateName'],
	'STREET_NAME': addressValues['StreetName'],
	'STREET_NAME_PRE_DIRECTIONAL': addressValues['StreetNamePreDirectional'],
	'STREET_NAME_PRE_MODIFIER': addressValues['StreetNamePreModifier'],
	'STREET_NAME_PRE_TYPE': addressValues['StreetNamePreType'],
	'STREET_NAME_POST_DIRECTIONAL': addressValues['StreetNamePostDirectional'],
	'STREET_NAME_POST_MODIFIER': addressValues['StreetNamePostModifier'],
	'STREET_NAME_POST_TYPE': addressValues['StreetNamePostType'],
	'SUBADDRESS_IDENTIFIER': addressValues['Subtagged_addressessIdentifier'],
	'SUBADDRESS_TYPE': addressValues['Subtagged_addressessType'],
	'USPS_BOX_GROUP_ID': addressValues['USPSBoxGroupID'],
	'USPS_BOX_GROUP_TYPE': addressValues['USPSBoxGroupType'],
	'USPS_BOX_ID': addressValues['USPSBoxID'],
	'USPS_BOX_TYPE': addressValues['USPSBoxType'],
	'ZIP_CODE': addressValues['ZipCode']
	})
				
def contstructAddressString(row):
	return(' '.join([row['RESIDENTIAL_ADDRESS1'], 
		   row['RESIDENTIAL_SECONDARY_ADDR'],"\n",	
		   row['RESIDENTIAL_CITY'],
		   row['RESIDENTIAL_STATE'],
		   row['RESIDENTIAL_ZIP']
		  ]))
		 

def appendMailingAddress(outrow, row):
	# Use residential address if no mailing address provided
	if not row['MAILING_ADDRESS1'] or not row['MAILING_CITY'] or not row['MAILING_STATE'] or not row['MAILING_ZIP']:
		outrow.update({
		'MAIL_ADDRESS_LINE1':row['RESIDENTIAL_ADDRESS1'],
		'MAIL_ADDRESS_LINE2':row['RESIDENTIAL_SECONDARY_ADDR'],
		'MAIL_CITY':row['RESIDENTIAL_CITY'],
		'MAIL_STATE':row['RESIDENTIAL_STATE'],
		'MAIL_ZIP_CODE':row['RESIDENTIAL_ZIP'],
		'MAIL_COUNTRY':'USA'
		})
	else:
		outrow.update({
		'MAIL_ADDRESS_LINE1':row['MAILING_ADDRESS1'],
		'MAIL_ADDRESS_LINE2':row['MAILING_SECONDARY_ADDRESS'],
		'MAIL_CITY':row['MAILING_CITY'],
		'MAIL_STATE':row['MAILING_STATE'],
		'MAIL_ZIP_CODE':row['MAILING_ZIP'],
		'MAIL_COUNTRY':row['MAILING_COUNTRY']
		})
		 
		  
if len(sys.argv) != 2:
	print('Usage: OhioPrepare <inputfile>');
	quit()
	
inputFile = sys.argv[1]
outputFile = re.sub('\\..*$', '_OUT.csv',inputFile, count=1)	
errorFileName = re.sub('\\..*$', '_ERR.csv',inputFile, count=1)	
with open(inputFile) as csvfile, \
	open(outputFile, 'w') as outfile, open(errorFileName, 'w') as errorFile:
		reader = csv.DictReader(csvfile, dialect='excel')
		fieldnames= [
			'ADDRESS_NUMBER',
			'ADDRESS_NUMBER_PREFIX',
			'ADDRESS_NUMBER_SUFFIX',
			'BUILDING_NAME',
			'CORNER_OF',
			'INTERSECTION_SEPARATOR',
			'LANDMARK_NAME',
			'NOT_ADDRESS',
			'OCCUPANCY_TYPE',
			'OCCUPANCY_IDENTIFIER',
			'PLACE_NAME',
			'STATE_NAME',
			'STREET_NAME',
			'STREET_NAME_PRE_DIRECTIONAL',
			'STREET_NAME_PRE_MODIFIER',
			'STREET_NAME_PRE_TYPE',
			'STREET_NAME_POST_DIRECTIONAL',
			'STREET_NAME_POST_MODIFIER',
			'STREET_NAME_POST_TYPE',
			'SUBADDRESS_IDENTIFIER',
			'SUBADDRESS_TYPE',
			'USPS_BOX_GROUP_ID',
			'USPS_BOX_GROUP_TYPE',
			'USPS_BOX_ID',
			'USPS_BOX_TYPE',
			'ZIP_CODE',
			'STATE_VOTER_REF',
			'COUNTY_VOTER_REF',
			'TITLE',
			'FIRST_NAME',
			'MIDDLE_NAME',
			'LAST_NAME',
			'NAME_SUFFIX',
			'GENDER',
			'BIRTHDATE',
			'REGISTRATION_DATE',
			'REGISTRATION_STATUS',
			'ABSTENTEE_TYPE',
			'PARTY',
			'EMAIL',
			'PHONE',
			'DO_NOT_CALL_STATUS',
			'LANGUAGE_CHOICE',
			'MAIL_ADDRESS_LINE1',
			'MAIL_ADDRESS_LINE2',
			'MAIL_CITY',
			'MAIL_STATE',
			'MAIL_ZIP_CODE',
			'MAIL_COUNTRY']
		writer = csv.DictWriter(outfile, fieldnames=fieldnames)
		writer.writeheader()
		
		
		# Create a file for writing addresses that don't parse		
		errnames = list(fieldnames)
		errnames.extend(['PARSED_STRING',"ORIGINAL_TEXT"])
		errWriter = csv.DictWriter(errorFile, fieldnames=errnames)
		errWriter.writeheader()	

		i = 0;
		for row in reader:
			addr = contstructAddressString(row)
				  
			outrow = {
					'STATE_VOTER_REF':row['SOS_VOTERID'],
					'COUNTY_VOTER_REF':row['COUNTY_ID'],
					'FIRST_NAME':row['FIRST_NAME'],
					'MIDDLE_NAME':row['MIDDLE_NAME'],
					'LAST_NAME':row['LAST_NAME'],
					'NAME_SUFFIX':row['SUFFIX'],
					'BIRTHDATE':row['DATE_OF_BIRTH'],
					'REGISTRATION_DATE':row['REGISTRATION_DATE'],
					'REGISTRATION_STATUS':row['VOTER_STATUS'],
					'PARTY':row['PARTY_AFFILIATION']
			}
			
			appendMailingAddress(outrow, row)
					
			try:
				tagged_address, address_type = usaddress.tag(addr)		

				# Transfer values to defaultdict so we can get nulls for missing fields
				addressValues = collections.defaultdict(str)
				for f in tagged_address:
					addressValues[f] = tagged_address[f]
				
				appendParsedFields(outrow, addressValues)
				writer.writerow(outrow)				

			### Get's thrown when the US Address parser gets hopelesly confused. Write this out to the errorFile
			### for manual training later
			except usaddress.RepeatedLabelError as e :
				print("\n\n--->Error\n",e.parsed_string, e.original_string)
				outrow.update({
					'PARSED_STRING':e.parsed_string,
					'ORIGINAL_TEXT':e.original_string
				})
				
				errWriter.writerow(outrow)					
				
			if i > 1000:
				break
			i += 1
			


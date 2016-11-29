import collections

def constructOutputFieldNames():
	return(['ADDRESS_NUMBER',
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
			'MAIL_COUNTRY',
			'COUNTYCODE',
			'CONGRESSIONAL_DIST',
			'UPPER_HOUSE_DIST',
			'LOWER_HOUSE_DIST',
			'PRECINCT',
			'COUNTY_BOARD_DIST',
			'SCHOOL_BOARD_DIST',
			'PRECINCT_SPLIT']
		)

# Transfer values to defaultdict so we can get nulls for missing fields
def	createDefaultDict(tagged_address):
	addressValues = collections.defaultdict(str)
	for f in tagged_address:
		addressValues[f] = tagged_address[f]
	return addressValues
	
	
def appendMailingAddressFromTaggedFields(outrow, tagged_address, address_type):
	addressValues = createDefaultDict(tagged_address)
	if address_type == 'Street Address':
		outrow.update({
			'MAIL_ADDRESS_LINE1':constructMailAddr1FromAddressValues(addressValues),
			'MAIL_ADDRESS_LINE2':constructMailAddr2FromAddressValues(addressValues)
			})
	elif address_type == 'PO Box':
		outrow.update({
			'MAIL_ADDRESS_LINE1':constructMailAddr1FromPOBoxAddressValues(addressValues)})			
			
	outrow.update({
		'MAIL_CITY':addressValues['PlaceName'],
		'MAIL_STATE':addressValues['StateName'],
		'MAIL_ZIP_CODE':addressValues['ZipCode'],
		'MAIL_COUNTRY':'USA'})
		

def appendParsedFields(outrow, tagged_address):
	addressValues = createDefaultDict(tagged_address)
		
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
	})
	
def constructMailAddr1FromOutRow(outrow):
		return ' '.join([outrow['ADDRESS_NUMBER_PREFIX'],
			outrow['ADDRESS_NUMBER'],
			outrow['ADDRESS_NUMBER_SUFFIX'],
			outrow['STREET_NAME_PRE_DIRECTIONAL'],
			outrow['STREET_NAME_PRE_MODIFIER'],
			outrow['STREET_NAME_PRE_TYPE'],
			outrow['STREET_NAME'],
			outrow['STREET_NAME_POST_TYPE'],
			outrow['STREET_NAME_POST_MODIFIER'],
			outrow['STREET_NAME_POST_DIRECTIONAL']
		])

def constructMailAddr1FromAddressValues(addressValues):
		return ' '.join([addressValues['AddressNumberPrefix'],
			addressValues['AddressNumber'],
			addressValues['AddressNumberSuffix'],
			addressValues['StreetNamePreDirectional'],
			addressValues['StreetNamePreModifier'],
			addressValues['StreetNamePreType'],
			addressValues['StreetName'],
			addressValues['StreetNamePostType'],
			addressValues['StreetNamePostModifier'],
			addressValues['StreetNamePostDirectional']
		])
def constructMailAddr2FromAddressValues(addressValues):
			return ' '.join([addressValues['OccupancyType'], addressValues['OccupancyIdentifier']])
		
def constructMailAddr1FromPOBoxAddressValues(addressValues):
			return ' '.join([addressValues['USPSBoxType'], addressValues['USPSBoxID']])
			
def constructMailAddr2FromOutRow(outrow):
		return ' '.join([outrow['OCCUPANCY_TYPE'],
			outrow['OCCUPANCY_IDENTIFIER']
		])
		
import csv
from io import TextIOWrapper
import os
import re
import sys
import zipfile

from src.main.python.transformers.base_transformer import BaseTransformer
import usaddress


class StatePreparer:
    """
    This class prepares the data to be parsed by row by the StateTransformer
    based on the state's data file download defaults.

    Pennsylvania voter files are provided as a zip file. If you purchase the
    entire state, it will be called Statewide.zip.  The zip file will contain
    three files per-county.  Examples:
    `MONTGOMERY FVE 20170102.txt`:
       The main voter file. 'FVE' presumably stands for 'Full Voter Export'.
    `MONTGOMERY Zone Types 20170102.txt`:
       Maps district column to district type
    `MONTGOMERY Zone Codes 20170102.txt`:
       Maps codes found in FVE file to a description and column

    """

    voter_file_re = re.compile(r'.+FVE.+\.txt')

    input_fields = [
        'STATE_VOTER_REF',
        'TITLE',
        'LAST_NAME',
        'FIRST_NAME',
        'MIDDLE_NAME',
        'NAME_SUFFIX',
        'GENDER',  # F=Female, M=Male, U=Unknown
        'BIRTHDATE',
        'REGISTRATION_DATE',
        'REGISTRATION_STATUS',  # A=Active, I=Inactive
        '_STATUS_CHANGE_DATE',
        '_PARTY_CODE',
        'ADDRESS_NUMBER',
        'ADDRESS_NUMBER_SUFFIX',
        'STREET_NAME',
        '_ADDRESS_APARTMENT_NUM',
        '_ADDRESS_LINE2',
        '_REGISTRATION_CITY',
        'STATE_NAME',
        'ZIP_CODE',
        'MAIL_ADDRESS_LINE1',
        'MAIL_ADDRESS_LINE2',
        'MAIL_CITY',
        'MAIL_STATE',
        'MAIL_ZIP_CODE',
        '_LASTVOTE',
        '_PRECINCT_CODE',  # TODO: confirm this matches to extract_precinct
        'PRECINCT_SPLIT',
        '_LAST_CHANGE_DATE',
        '_LEGACY_SYSTEM_ID'] \
        + ['_DISTRICT%d' % (d+1) for d in range(40)] \
        + ['_VOTEHISTORY_%d' % (d+1) for d in range(80)] \
        + ['PHONE', 'COUNTYCODE', 'MAIL_COUNTRY']

    def __init__(self, voter_zip_file_path=None, output_path=None):
        from src.main.python.transformers import DATA_DIR

        self.voter_zip_file_path = voter_zip_file_path \
                                   or os.path.join(DATA_DIR,
                                                   'Pennsylvania',
                                                   'Statewide.zip')
        self.output_path = output_path \
                           or os.path.join(DATA_DIR,
                                           'Pennsylvania',
                                           'voters_out.csv')

        self.transformer = StateTransformer(date_format="%m/%d/%Y", sep="\t",
                                            input_fields=self.input_fields)

    def process(self):
        z = zipfile.ZipFile(self.voter_zip_file_path)
        self.voters(z)

    def voters(self, zip_file):
        county_files = [f for f in zip_file.namelist()
                        if self.voter_file_re.match(f)]
        for county in county_files:
            self.process_county_zones(zip_file, county)
            self.transformer(zip_file.open(county), self.output_path)

    def process_county_zones(self, zip_handle, county_voter_filename):
        zonecode_file, zonetype_file = self.zone_files(county_voter_filename)
        self.transformer.load_county_zones(zip_handle.open(zonetype_file))

    def zone_files(self, voter_filename):
        if 'FVE' in voter_filename:
            return (re.sub(r' FVE ', ' Zone Codes ', voter_filename),
                    re.sub(r' FVE ', ' Zone Types ', voter_filename))
        else:
            return None, None

class StateTransformer(BaseTransformer):

    #roughly all affiliations that have 1000+ registrants
    party_map = {
        "D": "DEM",
        "R": "REP",
        "IND": "IDP",
        "LN": "LIB",
        "GR": "GRN",
        "": "UN",
        "I": "UN",
        "INDE": "UN",
        "NF": "UN",
        "NON": "UN",
        "NO": "UN",
        "NOP": "UN",
        "OTH": "OTH",
        "WFP": "WOR",
        "C": "CON",
    }

    # this will get filled by calling load_county_zones and will
    # have county-specific overrides for the zonecode_column_defaults
    zonecode_column_by_county = {}

    zonecode_column_defaults = {
        'congressional': 8,  #US congressional district
        'upper_house': 7,  # Pennsylvania Senate
        'lower_house': 6,  # Pennsylvania General Assembly legislature
        'county_board': 9,  # sometimes column 9 is school district
        'school_board': 3,
        'precinct': 1,
        'precinct_split': 13,
    }

    def open(self, input_path):
        """
        open() override to support zipfile handle being passed as `input_path`
        when the transformer is called
        """
        if isinstance(input_path, zipfile.ZipExtFile):
            return TextIOWrapper(input_path,
                                 encoding='utf8',
                                 errors='ignore', line_buffering=True)
        else:
            return BaseTransformer.open(self, input_path)

    def _set_county_zonetype(self, zonedict, key):
        self.zonecode_column_by_county.setdefault(zonedict['county'], {}).update({
            key: int(zonedict['column'])
        })

    def load_county_zones(self, zonetype_input_path):
        """
        Loads county-specific "District" column # for district-type columns
        """
        with self.open(zonetype_input_path) as zonetypes:
            reader = csv.DictReader(zonetypes,
                                    delimiter=self.sep,
                                    fieldnames=['county', 'column', 'prefix', 'zonetype'])
            for zonedict in reader:
                # Avoiding test for 'senat(e)' and 'legislature' because
                # there can be columns for US Senators, and not just
                # state senators. State senators are reliably default column 7
                if re.search('school', zonedict['zonetype'], re.I):
                    self._set_county_zonetype(zonedict, 'school_board')
                elif re.search('precinct split', zonedict['zonetype'], re.I):
                    self._set_county_zonetype(zonedict, 'precinct_split')
                elif re.search('precinct', zonedict['zonetype'], re.I):
                    self._set_county_zonetype(zonedict, 'precinct')
                elif re.search('^county$', zonedict['zonetype'], re.I):
                    # more careful with county, since there can be:
                    # "county wide" and "county council"
                    self._set_county_zonetype(zonedict, 'county_board')


    #### Contact methods ######################################################

    extract_name = BaseTransformer.map_extract_by_keys('TITLE',
                                                       'FIRST_NAME',
                                                       'MIDDLE_NAME',
                                                       'LAST_NAME',
                                                       'NAME_SUFFIX', defaults={
                                                           'FIRST_NAME': '_',
                                                           'LAST_NAME': '_',
                                                       })

    extract_email = lambda self, i: {'EMAIL': None}
    extract_phone_number = BaseTransformer.map_extract_by_keys('PHONE')
    extract_do_not_call_status = lambda self, i: {'DO_NOT_CALL_STATUS': None}

    #### Demographics methods #################################################

    extract_gender = BaseTransformer.map_extract_by_keys('GENDER', defaults={'GENDER':'U'})

    extract_race = lambda self, i: {'RACE': 'U'}
    extract_birth_state = lambda self, i: {'BIRTH_STATE': None, 'BIRTHDATE_IS_ESTIMATE': 'N'}

    def extract_birthdate(self, input_dict):
        return {'BIRTHDATE': self.convert_date(input_dict['BIRTHDATE'])}

    extract_language_choice = lambda self, i: {'LANGUAGE_CHOICE': None}

    #### Address methods ######################################################

    def extract_registration_address(self, input_dict):
        """
        Relies on the usaddress package.

        Call the self.convert_usaddress_dict() method on the output of
        usaddress.tag. We provide example code in the method to make this clear.

        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'ADDRESS_NUMBER'
                'ADDRESS_NUMBER_PREFIX'
                'ADDRESS_NUMBER_SUFFIX'
                'BUILDING_NAME'
                'CORNER_OF'
                'INTERSECTION_SEPARATOR'
                'LANDMARK_NAME'
                'NOT_ADDRESS'
                'OCCUPANCY_TYPE'
                'OCCUPANCY_IDENTIFIER'
                'PLACE_NAME'
                'STATE_NAME'
                'STREET_NAME'
                'STREET_NAME_PRE_DIRECTIONAL'
                'STREET_NAME_PRE_MODIFIER'
                'STREET_NAME_PRE_TYPE'
                'STREET_NAME_POST_DIRECTIONAL'
                'STREET_NAME_POST_MODIFIER'
                'STREET_NAME_POST_TYPE'
                'SUBADDRESS_IDENTIFIER'
                'SUBADDRESS_TYPE'
                'USPS_BOX_GROUP_ID'
                'USPS_BOX_GROUP_TYPE'
                'USPS_BOX_ID'
                'USPS_BOX_TYPE'
                'ZIP_CODE'
        """
        # columns to create address, in order
        address_components = [
            'ADDRESS_NUMBER',
            'ADDRESS_NUMBER_SUFFIX',
            'STREET_NAME'
        ]
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])
        if input_dict['_ADDRESS_APARTMENT_NUM']:
            address_str = '{} Apt {}'.format(address_str, input_dict['_ADDRESS_APARTMENT_NUM'])
        if input_dict['_ADDRESS_LINE2']:
            address_str = '{}, {}'.format(address_str, input_dict['_ADDRESS_LINE2'])

        # use the usaddress_tag method to handle errors
        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)
        # use the convert_usaddress_dict to get correct column names
        # and fill in missing values
        converted_addr = self.convert_usaddress_dict(usaddress_dict)
        converted_addr.update({
            'PLACE_NAME': input_dict['_REGISTRATION_CITY'],
            'STATE_NAME': input_dict['STATE_NAME'],
            'ZIP_CODE': input_dict['ZIP_CODE']
        })
        return converted_addr

    extract_county_code = BaseTransformer.map_extract_by_keys('COUNTYCODE')

    extract_mailing_address = BaseTransformer.map_extract_by_keys('MAIL_ADDRESS_LINE1',
                                                                  'MAIL_ADDRESS_LINE2',
                                                                  'MAIL_CITY',
                                                                  'MAIL_STATE',
                                                                  'MAIL_ZIP_CODE',
                                                                  'MAIL_COUNTRY')

    #### Political methods ####################################################

    extract_state_voter_ref = BaseTransformer.map_extract_by_keys('STATE_VOTER_REF')

    def extract_county_voter_ref(self, input_dict):
        return {'COUNTY_VOTER_REF': input_dict['STATE_VOTER_REF']}

    def extract_registration_date(self, input_dict):
        date = self.convert_date(input_dict['REGISTRATION_DATE'])
        return {'REGISTRATION_DATE': date}

    extract_registration_status = BaseTransformer.map_extract_by_keys('REGISTRATION_STATUS')
    extract_absentee_type = lambda self, i: {'ABSENTEE_TYPE': ''}

    def extract_party(self, input_dict):
        return {'PARTY': self.party_map.get(input_dict['_PARTY_CODE'])}

    # NOTE: District extractions below are leveraging what seems to be a
    # convention more than something required by the data files.
    # Each district type tends to be in a particular district field column.
    # However, technically, this is based on the *Zone Codes*.txt files
    # for each county file.  Unfortunately, there's even less reliability
    # for code names or descriptions.

    def extract_congressional_dist(self, input_dict):
        """
        gets the number out of a bunch of codes avoiding 0-prefix
        Examples: "CG14", "CON18TH", "CN04", "6USCD"
        """
        return {'CONGRESSIONAL_DIST': re.search(r'[1-9]\d*',
                                                input_dict['_DISTRICT8']).group(0)}

    def extract_upper_house_dist(self, input_dict):
        """
        gets the number out of a bunch of codes avoiding 0-prefix
        Examples: "S47", "SENT35", "9", "SE00046"
        and unintuitively "48SSGA" described as "48TH STATE SENATOR TO GENERAL ASSEMBLY"
        Even though it references the GA, it's still the *senator*
        """
        return {'UPPER_HOUSE_DIST': re.search(r'[1-9]\d*',
                                                input_dict['_DISTRICT7']).group(0)}

    def extract_lower_house_dist(self, input_dict):
        return {'LOWER_HOUSE_DIST': re.search(r'[1-9]\d*',
                                                input_dict['_DISTRICT6']).group(0)}

    def _zonecode_column(self, key, input_dict):
        """Gets the county-specific column for `key` with fallback to default column"""
        return self.zonecode_column_by_county.get(
            input_dict['COUNTYCODE'], {}).get(key,
                                                 self.zonecode_column_defaults.get(key))

    def extract_precinct(self, input_dict):
        """
        Inputs:
            input_dict: name or list of columns
        Outputs:
            Dictionary with following keys
                'PRECINCT'
        """
        column = self._zonecode_column('precinct', input_dict)
        return {'PRECINCT': input_dict['_DISTRICT%d' % column]}

    def extract_county_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: name or list of columns
        Outputs:
            Dictionary with following keys
                'COUNTY_BOARD_DIST'
        """
        column = self._zonecode_column('county_board', input_dict)
        return {'COUNTY_BOARD_DIST': input_dict['_DISTRICT%d' % column]}

    def extract_school_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: name or list of columns
        Outputs:
            Dictionary with following keys
                'SCHOOL_BOARD_DIST'
        """
        column = self._zonecode_column('school_board', input_dict)
        return {'SCHOOL_BOARD_DIST': input_dict['_DISTRICT%d' % column]}

    def extract_precinct_split(self, input_dict):
        """
        Inputs:
            input_dict: name or list of columns
        Outputs:
            Dictionary with following keys
                'PRECINCT_SPLIT'
        """
        column = self._zonecode_column('precinct_split', input_dict)
        return {'PRECINCT_SPLIT': input_dict['_DISTRICT%d' % column]}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

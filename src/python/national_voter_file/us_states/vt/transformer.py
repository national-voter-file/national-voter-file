import csv
import os
import re
import sys

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'vt-voter-files--SAMPLE.txt'


class StatePreparer(BasePreparer):

    state_path = 'vt' # Two letter code for state
    state_name='Vermont' # Name of state with no spaces. Use CamelCase
    sep='|' # The character used to delimit records

    def __init__(self, input_path, *args):
        super(StatePreparer, self).__init__(input_path, *args)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
            reader = self.dict_iterator(self.open(self.input_path))
            for row in reader:
                # import pprint
                # pp = pprint.PrettyPrinter(indent=4)
                # pp.pprint(row)
                # break
                yield row

class StateTransformer(BaseTransformer):
    date_format='%m/%d/%Y' # The format used for dates
    input_fields = None # This can be a list of column names for the input file.
                        # Use None if the file has headers

    col_map = {
        'TITLE': None,
        'FIRST_NAME': 'First Name',
        'LAST_NAME': 'Last Name',
        'MIDDLE_NAME': 'Middle Name',
        'NAME_SUFFIX': 'Suffix',
        'GENDER': None,
        'PARTY': None,
        'RACE': None,
        'BIRTH_STATE': None,
        'COUNTY_BOARD_DIST': None,
        'LANGUAGE_CHOICE': None,
        'EMAIL': None,
        'PHONE': 'Telephone',
        'DO_NOT_CALL_STATUS': None,
        'STATE_VOTER_REF': 'VoterID',
        'COUNTY_VOTER_REF': None,
        'ABSENTEE_TYPE': None,
        'PRECINCT': None,
        'PRECINCT_SPLIT': None,
        'REGISTRATION_STATUS': 'Status',
        'SCHOOL_BOARD_DIST': 'School District',
        'UPPER_HOUSE_DIST': 'Senate District',
        'LOWER_HOUSE_DIST': 'Voting District',
    }

    col_type_dict = BaseTransformer.col_type_dict.copy()
    col_type_dict['PRECINCT_SPLIT'] = set([str, type(None)])
    col_type_dict['PRECINCT'] = set([str, type(None)])

    #### Contact methods #######################################################


    #### Demographics methods ##################################################

    def extract_birthdate(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'BIRTHDATE'
        """
        return {
            'BIRTHDATE': self.convert_date(date_str=input_columns['Year of Birth'], date_format='%Y'),
            'BIRTHDATE_IS_ESTIMATE': 'Y'
        }


    #### Address methods #######################################################

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

        columns = [
            'Legal Address Line 1',
            'Legal Address Line 2',
            'Legal Address City',
            'Legal Address State',
            'Legal Address Zip'
        ]

        mail_str = ' '.join([input_dict[x] for x in columns if input_dict[x] is not None])
        usaddress_dict, usaddress_type = self.usaddress_tag(mail_str)

        raw_dict = {
            'RAW_ADDR1': input_dict['Legal Address Line 1'],
            'RAW_ADDR2': input_dict['Legal Address Line 2'],
            'RAW_CITY': input_dict['Legal Address City'],
            'RAW_ZIP': input_dict['Legal Address Zip'],
        }

        for r in ['RAW_ADDR1', 'RAW_ADDR2']:
            if not raw_dict[r].strip():
                raw_dict[r] = '--Not provided--'

        if usaddress_dict:
            converted_addr = self.convert_usaddress_dict(usaddress_dict)
            validation_status = '2'
        else:
            converted_addr = self.constructEmptyResidentialAddress()
            validation_status = '1'

        converted_addr.update(raw_dict)

        state_name = input_dict['Legal Address State']
        if len(state_name.strip()) == 0:
            state_name = 'VT'

        converted_addr.update({
            'STATE_NAME': state_name,
            'VALIDATION_STATUS': validation_status
        })

        return converted_addr

    def extract_county_code(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTYCODE'
        """

        # Note this mapping is the same as the abbreviations the voter file 
        # uses when referring to Senate districts, with the exception of
        # Essex and Orelans which share a Senate district (ESX-ORL)
        county_code_map = {
            'ADDISON': 'ADD',
            'BENNINGTON': 'BEN',
            'CALEDONIA': 'CAL',
            'CHITTENDEN': 'CHI',
            'ESSEX': 'ESX',
            'FRANKLIN': 'FRA',
            'GRAND ISLE': 'GI',
            'LAMOILLE': 'LAM',
            'ORANGE': 'ORA',
            'ORLEANS': 'ORL',
            'RUTLAND': 'RUT',
            'WASHINGTON': 'WAV',
            'WINDHAM': 'WDH',
            'WINDSOR': 'WDR',
        }
        return {'COUNTYCODE': county_code_map[input_dict.get('County')]}

    def extract_mailing_address(self, input_dict):
        """
        Relies on the usaddress package.

        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'MAIL_ADDRESS_LINE1'
                'MAIL_ADDRESS_LINE2'
                'MAIL_CITY'
                'MAIL_STATE'
                'MAIL_ZIP_CODE'
                'MAIL_COUNTRY'
        """
        columns = [
            'Mailing Address Line 1',
            'Mailing Address Line 2',
            'Mailing Address City',
            'Mailing Address State',
            'Mailing Address Zip'
        ]

        mail_str = ' '.join([input_dict[x] for x in columns if input_dict[x] is not None])
        usaddress_dict, usaddress_type = self.usaddress_tag(mail_str)

        mail_addr_dict = {}

        if usaddress_type in ['Ambiguous', None]:
            print('Warn - {}: Ambiguous mailing address, falling back to residential'.format(usaddress_type))
        else:
            mail_addr_dict = {
                'MAIL_ADDRESS_LINE1': self.construct_mail_address_1(
                    usaddress_dict,
                    usaddress_type,
                ),
                'MAIL_ADDRESS_LINE2': self.construct_mail_address_2(usaddress_dict),
                'MAIL_CITY': usaddress_dict.get('PlaceName', None),
                'MAIL_ZIP_CODE': usaddress_dict.get('ZipCode', None),
                'MAIL_STATE': usaddress_dict.get('StateName', None),
                'MAIL_COUNTRY': 'USA',
            }

        return mail_addr_dict


    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF' : 'VT' + input_dict['VoterID']}

    def extract_registration_date(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'REGISTRATION_DATE'
        """
        return {'REGISTRATION_DATE': self.convert_date(input_columns['Date of Registration'])}

    def extract_congressional_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'CONGRESSIONAL_DIST'
        """

        # FIXME: Vermont only has 1 at-large Congressional District. Is this OK as a default value?
        return {'CONGRESSIONAL_DIST': 'AL'}


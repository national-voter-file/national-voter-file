import csv
import os
import re
import sys
from datetime import date

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'DE_Sample.csv'

class StatePreparer(BasePreparer):
    state_path = 'de' # Two letter code for state
    state_name = 'Delaware' # Name of state with no spaces. Use CamelCase
    sep = ',' # The character used to delimit records


    def __init__(self, input_path, *args):
        super(StatePreparer, self).__init__(input_path, *args)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        reader = self.dict_iterator(self.open(self.input_path))
        for row in reader:
            yield row

class StateTransformer(BaseTransformer):
    date_format = "%Y%m%d"
    input_fields = None # This can be a list of column names for the input file.
                        # Use None if the file has headers
    col_map = {
        'TITLE': None,
        'FIRST_NAME': 'FIRST-NAME',
        'LAST_NAME': 'LAST-NAME',
        'MIDDLE_NAME': None,
        'NAME_SUFFIX': 'SUFFIX',
        'GENDER': None,
        'RACE': None,
        'BIRTH_STATE': None,
        'LANGUAGE_CHOICE': None,
        'EMAIL': None,
        'PHONE': None,
        'DO_NOT_CALL_STATUS': None,
        'COUNTYCODE': 'COUNTY',
        'STATE_VOTER_REF': 'UNIQUE-ID',
        'COUNTY_VOTER_REF': None,
        'ABSENTEE_TYPE': None,
        'PRECINCT': None,
        'PRECINCT_SPLIT': None,
        'REGISTRATION_STATUS': 'STATUS',
        'SCHOOL_BOARD_DIST': 'SCH-DIST',
        'UPPER_HOUSE_DIST': 'SD',
        'LOWER_HOUSE_DIST': 'RD',
        'CONGRESSIONAL_DIST': 'ED'
    }
    de_party_map = {
        'D': 'DEM',
        'R': 'REP',
        'I': 'UN',
        'L': 'LIB',
        'A': 'AI',
        'B': 'FED',
        'C': 'CIT',
        'E': 'LIB',
        'F': 'NEW',
        'K': 'DEL',
        'N': 'ALI',
        'O': 'OTH',
        'P': 'TAX',
        'S': 'STS',
        'U': 'UNI',
        'W': 'IND',
        'X': 'ROL',
        'Y': 'BLU',
        'Q': 'AMC',
        'V': 'NLP',
        'M': 'REF',
        'H': 'GRN',
        'J': 'WOR',
        'T': 'CON',
        'Z': 'SP',
    }

    col_type_dict = BaseTransformer.col_type_dict.copy()
    col_type_dict['PRECINCT_SPLIT'] = set([str, type(None)])
    col_type_dict['PRECINCT'] = set([str, type(None)])
    col_type_dict['STATE_NAME'] = set([str, type(None)])
    #### Demographics methods ##################################################

    def extract_birthdate(self, input_dict):
        """
        Inputs:
            input_dict: names of columns and corresponding values
        Outputs:
            Dictionary with following keys
                'BIRTHDATE'
        """
        return {
            'BIRTHDATE': date(int(input_dict['YEAR-OF-BIRTH']), 1, 1),
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

        # columns to create address, in order
        address_components = [
            'HOME-NO',
            'HOME-STREET',
            'HOME-APT',
            'HOME-DEV',
        ]
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])

        raw_dict = {
            'RAW_ADDR1': address_str,
            # Including Raw Addr 2 as same because not as clear of a division
            'RAW_ADDR2': address_str,
            'RAW_CITY': input_dict['HOME-CITY'],
            'RAW_ZIP': input_dict['HOME-ZIPCODE']
        }

        for r in ['RAW_ADDR1', 'RAW_ADDR2']:
            if not raw_dict[r].strip():
                raw_dict[r] = '--Not provided--'

        usaddress_dict = self.usaddress_tag(address_str)[0]

        if usaddress_dict:
            converted_addr = self.convert_usaddress_dict(usaddress_dict)

            converted_addr.update({
                'PLACE_NAME': input_dict['HOME-CITY'],
                'ZIP_CODE': input_dict['HOME-ZIPCODE'],
                'VALIDATION_STATUS': '2'
            })
            converted_addr.update(raw_dict)
        else:
            converted_addr = self.constructEmptyResidentialAddress()
            converted_addr.update(raw_dict)
            converted_addr.update({
                'STATE_NAME': input_dict['STATE'],
                'VALIDATION_STATUS': '1'
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
        return {'COUNTYCODE': input_dict['COUNTY']}

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


        address_components = [
            "MAIL-NO",
            "MAIL-APT",
            "MAIL-STR",
            "MAIL-CITY",
            "MAIL-STATE",
            "MAIL-ZIP"
        ]
        mail_str = ' '.join([input_dict[x] for x in address_components if input_dict[x] is not None])

        mail_addr_dict = {}

        if mail_str.strip():
            usaddress_dict, usaddress_type = self.usaddress_tag(mail_str)

            if usaddress_type == 'Ambiguous':
                print('Warn - {}: Ambiguous mailing address, falling back to residential'.format(usaddress_type))
            elif usaddress_dict:
                mail_addr_dict = {
                    'MAIL_ADDRESS_LINE1': self.construct_mail_address_1(
                        usaddress_dict, usaddress_type
                    ),
                    'MAIL_ADDRESS_LINE2': self.construct_mail_address_2(usaddress_dict),
                    'MAIL_CITY': usaddress_dict.get('PlaceName', None),
                    'MAIL_ZIP_CODE': usaddress_dict.get('ZipCode', None),
                    'MAIL_STATE': usaddress_dict.get('StateName', None),
                    'MAIL_COUNTRY': 'USA'
                }
        return mail_addr_dict


    #### Political methods #####################################################


    def extract_registration_date(self, input_dict):
        """
        Inputs:
            input_dict: names of columns and corresponding values
        Outputs:
            Dictionary with following keys
                'REGISTRATION_DATE'
        """

        try:
            return {'REGISTRATION_DATE': self.convert_date(input_dict['DATE-REG'])}
        except ValueError:
            return {'REGISTRATION_DATE': self.convert_date(input_dict['DATE-REG'], '%Y%m00')}

    def extract_party(self, input_dict):
        """
        Inputs:
            input_dict: names of columns and corresponding values
        Outputs:
            Dictionary with following keys
                'PARTY'
        """

        return {'PARTY' : self.de_party_map.get(input_dict['PARTY'], input_dict['PARTY'])}

    def extract_county_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: names of columns and corresponding values
        Outputs:
            Dictionary with following keys
                'COUNTY_BOARD_DIST'
        """
        return {'COUNTY_BOARD_DIST': input_dict['CNLEVY']}

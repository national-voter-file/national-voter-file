import csv
import io
import os
import re
import sys

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'voters.txt'

class StatePreparer(BasePreparer):

    state_path = 'ut' # Two letter code for state
    state_name = 'Utah' # Name of state with no spaces. Use CamelCase
    sep = ',' # The character used to delimit records

    def __init__(self, input_path, *args):
        super(StatePreparer, self).__init__(input_path, *args)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        fp = open(self.input_path, 'r', encoding='utf-8-sig')

        reader = self.dict_iterator(fp)
        for row in reader:
            yield row

class StateTransformer(BaseTransformer):
    date_format = '%m/%d/%Y' # The format used for dates
    input_fields = None # This can be a list of column names for the input file.
                        # Use None if the file has headers

    col_map = {
        'TITLE': None,
        'FIRST_NAME': 'First Name',
        'MIDDLE_NAME': 'Middle Name',
        'LAST_NAME': 'Last Name',
        'NAME_SUFFIX': 'Name Suffix',
        'GENDER': None,
        'RACE': None,
        'BIRTH_STATE': None,
        'LANGUAGE_CHOICE': None,
        'EMAIL': None,
        'PHONE': 'Phone',
        'DO_NOT_CALL_STATUS': None,
        'ABSENTEE_TYPE': 'Absentee',
        'CONGRESSIONAL_DIST': 'Congressional',
        'UPPER_HOUSE_DIST': 'State Senate',
        'LOWER_HOUSE_DIST': 'State House',
        'SCHOOL_BOARD_DIST': 'State Schoolboard',
        'COUNTY_BOARD_DIST': None,
        'COUNTYCODE': 'County ID',
        'COUNTY_VOTER_REF': None,
        'PRECINCT': 'Precinct',
        'PRECINCT_SPLIT': None,
    }

    ut_party_map = {
        # Commented values appeared in the data file but couldn't be mapped
        'Republican': 'REP',
        'Unaffiliated': 'UN',
        'Democratic': 'DEM',
        'Libertarian': 'LIB',
        'Independent American': 'AI',
        'Constitution': 'AMC',
        'Independent': 'UN',
        'Other': 'UN',
        'Green': 'GRN',
        'Personal Choice': 'PCP',
        'Americans Elect': 'AE',
        'Reform': 'REF',
        'Natural Law': 'NLP',
        'Socialist Workers': 'SWP',
        'Socialist': 'SP',
        'Utah Justice Party': 'UJP',
        'U.S. Taxpayers': 'TAX',
        'Peace and Freedom': 'PF',
        'Independent Patriot Party Of Utah': 'IPU',
        'Independent Patriot Party of Utah': 'IPU',
        'Desert Greens': 'GPU',
        #'American': '',
        #'Populist': '',
        #'Independents for Economic Recovery': '',
    }

    col_type_dict = BaseTransformer.col_type_dict.copy()
    # File contains some missing First Name values
    col_type_dict['FIRST_NAME'] = set([str, type(None)])
    col_type_dict['PRECINCT_SPLIT'] = set([str, type(None)])

    #### Demographics methods ##################################################

    def extract_birthdate(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'BIRTHDATE'
        """
        dob = None
        try:
            dob = self.convert_date(input_columns['DOB'])
        except ValueError:
            # Some rows have invalid date values
            pass

        return {
            'BIRTHDATE': dob,
            'BIRTHDATE_IS_ESTIMATE': 'N',
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
            'House Number', 'House Number Suffix', 'Direction Prefix', 'Street', 'Direction Suffix',
            'Street Type', 'Unit Type', 'Unit Number',
        ]
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])
        raw_dict = {
            'RAW_ADDR1': address_str,
            'RAW_ADDR2': address_str,
            'RAW_CITY': input_dict['City'],
            'RAW_ZIP': input_dict['Zip'],
        }

        # use the usaddress_tag method to handle errors
        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)
        # use the convert_usaddress_dict to get correct column names
        # and fill in missing values

        if usaddress_dict:
            converted = self.convert_usaddress_dict(usaddress_dict)
            converted['VALIDATION_STATUS'] = '2'  # ??
        else:
            converted = self.constructEmptyResidentialAddress()
            converted['VALIDATION_STATUS'] = '1'

        converted.update(raw_dict)
        converted['STATE_NAME'] = 'UT'
        return converted

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
        columns = ['Mailing Address']
        mail_str = ' '.join([x for x in columns])
        usaddress_dict, usaddress_type = self.usaddress_tag(mail_str)

        city = state = zipcode = None
        city_state_zip = input_dict['Mailing city, state  zip']
        try:
            if ',' in city_state_zip:
                city, state_zip = input_dict['Mailing city, state  zip'].split(',')
                state_zip = state_zip.split()
                if len(state_zip) == 2:
                    state, zipcode = state_zip
                elif state_zip:
                    state = None
                    zipcode = state_zip[-1]
        except ValueError:
            # Some rows have weird values for this field
            pass

        return {
            'MAIL_ADDRESS_LINE1': self.construct_mail_address_1(
                usaddress_dict,
                usaddress_type,
            ),
            'MAIL_ADDRESS_LINE2': self.construct_mail_address_2(usaddress_dict),
            'MAIL_CITY': city,
            'MAIL_ZIP_CODE': zipcode,
            'MAIL_STATE': state,
            'MAIL_COUNTRY': 'US',
        }

    #### Political methods #####################################################
    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF' : 'UT' + input_dict['Voter ID']}

    def extract_registration_date(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'REGISTRATION_DATE'
        """
        d = None
        try:
            d = self.convert_date(input_columns['Registration Date'])
        except ValueError:
            # Some rows have invalid date values
            pass

        return {'REGISTRATION_DATE': d}

    def extract_registration_status(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'REGISTRATION_STATUS'
        """
        return {'REGISTRATION_STATUS': input_columns['Status']}

    def extract_party(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PARTY'
        """
        party = input_columns['Party']
        return {'PARTY': self.ut_party_map.get(party)}

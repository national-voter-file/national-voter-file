import csv
import os
import re
import sys

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress
import datetime

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'OK_Sample.csv'

class StatePreparer(BasePreparer):

    state_path = 'ok'
    state_name='Oklahoma'
    sep = ","

    def __init__(self, input_path, *args):
        super(StatePreparer, self).__init__(input_path, *args)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
            reader = self.dict_iterator(self.open(self.input_path))
            for row in reader:
                yield row


class StateTransformer(BaseTransformer):
    date_format="%m/%d/%Y"

    input_fields = None

    oklahoma_party_map = {
        "DEM": "DEM",
        "REP": "REP",
        "IND": "UN",
        "LIB": "LIB",
        "AE": "AE"
    }

    col_type_dict = BaseTransformer.col_type_dict.copy()
    col_type_dict['PRECINCT_SPLIT'] = set([str, type(None)])

    col_map = {
        'PRECINCT_SPLIT': None
    }


    #### Contact methods #######################################################

    def extract_name(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'TITLE'
                'FIRST_NAME'
                'MIDDLE_NAME'
                'LAST_NAME'
                'NAME_SUFFIX'
        """
        output_dict = {
            'TITLE': None,
            'FIRST_NAME': input_dict['FirstName'],
            'MIDDLE_NAME': input_dict['MiddleName'],
            'LAST_NAME': input_dict['LastName'],
            'NAME_SUFFIX': input_dict['Suffix'],
        }

        return output_dict

    def extract_email(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'EMAIL'
        """
        return {'EMAIL': None}

    def extract_phone_number(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PHONE'
        """
        return {'PHONE': ""}

    def extract_do_not_call_status(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'DO_NOT_CALL_STATUS'
        """
        return {'DO_NOT_CALL_STATUS': None}

    #### Demographics methods ##################################################

    def extract_gender(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'GENDER'
        """

        return {'GENDER': "U"}

    def extract_race(self, input_dict):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'RACE'
        """

        return {'RACE': "U"}

    def extract_birth_state(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'BIRTH_STATE'
        """
        return {'BIRTH_STATE': None}

    def extract_birthdate(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'BIRTHDATE'
        """
        if len(input_dict['DateOfBirth']) > 0:
            date = self.convert_date(input_dict['DateOfBirth'])
        else:
            date = None
        return {'BIRTHDATE': date, 'BIRTHDATE_IS_ESTIMATE': 'N'}

    def extract_language_choice(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'LANGUAGE_CHOICE'
        """
        return {'LANGUAGE_CHOICE': None}

    #### Address methods #######################################################

    def extract_registration_address(self, input_dict):
        """
        Relies on the usaddress package.

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
        address_components = [
             'StreetNum',
             'StreetDir',
             'StreetName',
             'StreetType',
             'BldgNum'
        ]
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])

        raw_dict = {
            'RAW_ADDR1': ' '.join([
             input_dict['StreetNum'],
             input_dict['StreetDir'],
             input_dict['StreetName'],
             input_dict['StreetType'],
             input_dict['BldgNum']
            ]),
            'RAW_ADDR2': "",
            'RAW_CITY': input_dict['City'],
            'RAW_ZIP': input_dict['Zip']
        }

        if(not raw_dict['RAW_ADDR1'].strip()):
            raw_dict['RAW_ADDR1'] = '--Not provided--'

        # OK doesn't have residence state
        state_name = 'OK'

        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)

        if(usaddress_dict):
            converted_addr = self.convert_usaddress_dict(usaddress_dict)

            converted_addr.update({'PLACE_NAME': raw_dict['RAW_CITY'],
                                   'STATE_NAME': state_name,
                                   'ZIP_CODE': raw_dict['RAW_ZIP'],
                                   'VALIDATION_STATUS':'2'
            })

            converted_addr.update(raw_dict)
        else:
            converted_addr = self.constructEmptyResidentialAddress()
            converted_addr.update(raw_dict)
            converted_addr.update({
                'STATE_NAME': state_name,
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
        #First 2 numbers of Precint are the county code.
        return {'COUNTYCODE': input_dict['Precinct'][:2]}

    def extract_mailing_address(self, input_dict):
        """
        Relies on the usaddress package.

        We provide template code.

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

        if( input_dict['MailStreet1'].strip() and input_dict['MailCity'].strip()):
            return {
                'MAIL_ADDRESS_LINE1': input_dict['MailStreet1'],
                'MAIL_ADDRESS_LINE2': " ".join([
                    input_dict['MailStreet2'],
                    input_dict['MailStreet2']]),
                'MAIL_CITY': input_dict['MailCity'],
                'MAIL_STATE': input_dict['MailState'],
                'MAIL_ZIP_CODE': input_dict['MailZip'],
                'MAIL_COUNTRY': "USA"
            }
        else:
            return {}

    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'STATE_VOTER_REF'
        """
        return {'STATE_VOTER_REF': "OK"+input_dict['VoterID']}

    def extract_county_voter_ref(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTY_VOTER_REF'
        """
        return {'COUNTY_VOTER_REF': None}

    def extract_registration_date(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'REGISTRATION_DATE'
        """
        if len(input_dict['OriginalRegistration']) > 0:
            date = self.convert_date(input_dict['OriginalRegistration'])
        else:
            date = None
        return {'REGISTRATION_DATE': date}

    def extract_registration_status(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'REGISTRATION_STATUS'
        """
        return {'REGISTRATION_STATUS': input_dict['Status']}

    def extract_absentee_type(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'ABSTENTEE_TYPE'
        """
        return {'ABSENTEE_TYPE': None}

    def extract_party(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PARTY'
        """
        party = input_dict['PolitalAff']
        return {'PARTY': self.oklahoma_party_map[party]}


    def extract_congressional_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'CONGRESSIONAL_DIST'
        """
        return {'CONGRESSIONAL_DIST': None}

    def extract_upper_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'UPPER_HOUSE_DIST'
        """
        return {'UPPER_HOUSE_DIST': None}

    def extract_lower_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'LOWER_HOUSE_DIST'
        """
        return {'LOWER_HOUSE_DIST': None}

    def extract_precinct(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT'
                'PRECINCT_SPLIT'

        """
        split = input_dict['Precinct']

        return {
                'PRECINCT': input_dict['Precinct'],
                'PRECINCT_SPLIT': split
            }


    def extract_county_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTY_BOARD_DIST'
        """
        # Not sure if mapping exists, verify
        return {'COUNTY_BOARD_DIST': input_dict['CountyComm']}

    def extract_school_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'SCHOOL_BOARD_DIST'
        """
        # Not sure if mapping exists, verify
        return {'SCHOOL_BOARD_DIST': input_dict['School']}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

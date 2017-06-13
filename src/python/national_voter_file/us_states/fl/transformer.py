import csv
import os
import re
import sys
import datetime

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'AllFLSample20160908.txt'

class StatePreparer(BasePreparer):
    state_path = 'fl'
    state_name = 'Florida'
    sep = "\t"

    def __init__(self, input_path, *args, **kwargs):
        super(StatePreparer, self).__init__(input_path, *args, **kwargs)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        reader = self.dict_iterator(self.open(self.input_path))
        for row in reader:
            yield row


class StateTransformer(BaseTransformer):
    date_format = "%m/%d/%Y"

    col_type_dict = BaseTransformer.col_type_dict.copy()

    col_map = {

    }

    input_fields = [
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
        'Email address'
    ]

    florida_party_map = {
        'AIP':'AI',
        'AMP':'AMP',
        'CPF':'AMC',
        'DEM':'DEM',
        'ECO':'ECO',
        'GRE':'GRN',
        'IDP':'IDP',
        'INT':'AI',
        'LPF':'LIB',
        'NPA':'UN',
        'PSL':'PSL',
        'REF':'REF',
        'REP':'REP',
        'IND':'IPF',
        ' ':'UN',
        '':'UN'
    }

    florida_race_map = {
        '1':'I',
        '2':'A',
        '3':'B',
        '4':'H',
        '5':'W',
        '6':'O',
        '7':'M',
        '9':'U'
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
            'FIRST_NAME': input_dict['Name First'],
            'MIDDLE_NAME': input_dict['Name Middle'],
            'LAST_NAME': input_dict['Name Last'],
            'NAME_SUFFIX': input_dict['Name Suffix'],
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
        return {'EMAIL': input_dict['Email address']}

    def extract_phone_number(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PHONE'
        """
        if input_dict['Daytime Phone Number'].strip():
            phone = "("+input_dict['Daytime Area Code']+") "+input_dict['Daytime Phone Number']
            if input_dict['Daytime Phone Extension'].strip():
                phone = phone + " x "+input_dict['Daytime Phone Extension'].strip()

            return {'PHONE': phone}
        else:
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
        gender = input_dict['Gender'].strip()
        if len(gender) == 0:
            gender = 'U'

        return {'GENDER': gender}

    def extract_race(self, input_dict):
        """
        Inputs:
            input_dict: names of columns and corresponding values
        Outputs:
            Dictionary with following keys
                'RACE'
        """
        race = input_dict['Race'].strip()
        return {'RACE': self.florida_race_map[race]}

    def extract_birth_state(self, input_dict):
        """
        Inputs:
            input_dict: names of columns and corresponding values
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
        # Some rows in FL have asterisks through them, empty birth date, so
        # allowing blanks here
        if len(input_dict['Birth Date'].strip()) > 0:
            birth_date = self.convert_date(input_dict['Birth Date'])
        else:
            birth_date = None

        return {
            'BIRTHDATE': birth_date,
            'BIRTHDATE_IS_ESTIMATE':'N'
        }

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
        address_components = [
            'Residence Address Line 1',
            'Residence Address Line 2'
        ]

        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])

        raw_dict = {
            'RAW_ADDR1': input_dict['Residence Address Line 1'],
            'RAW_ADDR2': input_dict['Residence Address Line 2'],
            'RAW_CITY': input_dict['Residence City (USPS)'],
            'RAW_ZIP': input_dict['Residence Zipcode']
        }

        # FL leaves state column blank frequently
        state_name = input_dict['Residence State']
        if len(state_name.strip()) == 0:
            state_name = 'FL'

        usaddress_dict = self.usaddress_tag(address_str)[0]

        if usaddress_dict:
            converted_addr = self.convert_usaddress_dict(usaddress_dict)

            converted_addr.update({
                'PLACE_NAME': raw_dict['RAW_CITY'],
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
        return {'COUNTYCODE': input_dict['County Code']}

    def extract_mailing_address(self, input_dict):
        if input_dict['Mailing Address Line 1'].strip() and input_dict['Mailing City'].strip():
            return {
                'MAIL_ADDRESS_LINE1': input_dict['Mailing Address Line 1'],
                'MAIL_ADDRESS_LINE2': " ".join([
                    input_dict['Mailing Address Line 2'],
                    input_dict['Mailing Address Line 2']]),
                'MAIL_CITY': input_dict['Mailing City'],
                'MAIL_STATE': input_dict['Mailing State'],
                'MAIL_ZIP_CODE': input_dict['Mailing Zipcode'],
                'MAIL_COUNTRY': input_dict['Mailing Country'] if input_dict['Mailing Country']  else "USA"
            }
        else:
            return {}

    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF': "FL" + input_dict['Voter ID']}

    def extract_county_voter_ref(self, input_dict):
        return {'COUNTY_VOTER_REF': None}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE': self.convert_date(input_dict['Registration Date'])}

    def extract_registration_status(self, input_dict):
        return {'REGISTRATION_STATUS': input_dict['Voter Status']}

    def extract_absentee_type(self, input_dict):
        return {'ABSENTEE_TYPE': None}

    def extract_party(self, input_dict):
        return {'PARTY': self.florida_party_map[input_dict['Party Affiliation']]}


    def extract_congressional_dist(self, input_dict):
        return {'CONGRESSIONAL_DIST': input_dict['Congressional District']}

    def extract_upper_house_dist(self, input_dict):
        return {'UPPER_HOUSE_DIST': input_dict['Senate District']}

    def extract_lower_house_dist(self, input_dict):
        return {'LOWER_HOUSE_DIST': input_dict['House District']}

    def extract_precinct(self, input_dict):
        return {
            'PRECINCT': input_dict['Precinct'],
        }

    def extract_precinct_split(self, input_dict):
        split = input_dict['Precinct Split'] if input_dict['Precinct Split'].strip() else input_dict['Precinct']
        return{
            'PRECINCT_SPLIT': split
        }

    def extract_county_board_dist(self, input_dict):
        # TODO: Not sure if mapping exists, verify
        return {'COUNTY_BOARD_DIST': None}

    def extract_school_board_dist(self, input_dict):
        # TODO: Not sure if mapping exists, verify
        return {'SCHOOL_BOARD_DIST': None}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

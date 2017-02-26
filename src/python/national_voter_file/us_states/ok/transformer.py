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

default_file = 'OK_Sample.csv'

class StatePreparer(BasePreparer):
    state_path = 'ok'
    state_name = 'Oklahoma'
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
    date_format = "%m/%d/%Y"

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
        return {
            'TITLE': None,
            'FIRST_NAME': input_dict['FirstName'],
            'MIDDLE_NAME': input_dict['MiddleName'],
            'LAST_NAME': input_dict['LastName'],
            'NAME_SUFFIX': input_dict['Suffix'],
        }

    def extract_email(self, input_dict):
        return {'EMAIL': None}

    def extract_phone_number(self, input_dict):
        return {'PHONE': None}

    def extract_do_not_call_status(self, input_dict):
        return {'DO_NOT_CALL_STATUS': None}

    #### Demographics methods ##################################################

    def extract_gender(self, input_dict):
        return {'GENDER': None}

    def extract_race(self, input_dict):
        return {'RACE': None}

    def extract_birth_state(self, input_dict):
        return {'BIRTH_STATE': None}

    def extract_birthdate(self, input_dict):
        if len(input_dict['DateOfBirth']) > 0:
            date = self.convert_date(input_dict['DateOfBirth'])
        else:
            date = None
        return {'BIRTHDATE': date, 'BIRTHDATE_IS_ESTIMATE': 'N'}

    def extract_language_choice(self, input_dict):
        return {'LANGUAGE_CHOICE': None}

    #### Address methods #######################################################

    def extract_registration_address(self, input_dict):
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

        if not raw_dict['RAW_ADDR1'].strip():
            raw_dict['RAW_ADDR1'] = '--Not provided--'

        # OK doesn't have residence state
        state_name = 'OK'

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
        # First 2 numbers of Precint are the county code.
        return {'COUNTYCODE': input_dict['Precinct'][:2]}

    def extract_mailing_address(self, input_dict):
        if input_dict['MailStreet1'].strip() and input_dict['MailCity'].strip():
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
        return {'STATE_VOTER_REF': "OK" + input_dict['VoterID']}

    def extract_county_voter_ref(self, input_dict):
        return {'COUNTY_VOTER_REF': None}

    def extract_registration_date(self, input_dict):
        if len(input_dict['OriginalRegistration']) > 0:
            date = self.convert_date(input_dict['OriginalRegistration'])
        else:
            date = None
        return {'REGISTRATION_DATE': date}

    def extract_registration_status(self, input_dict):
        return {'REGISTRATION_STATUS': input_dict['Status']}

    def extract_absentee_type(self, input_dict):
        return {'ABSENTEE_TYPE': None}

    def extract_party(self, input_dict):
        return {'PARTY': self.oklahoma_party_map[input_dict['PolitalAff']]}


    def extract_congressional_dist(self, input_dict):
        return {'CONGRESSIONAL_DIST': None}

    def extract_upper_house_dist(self, input_dict):
        return {'UPPER_HOUSE_DIST': None}

    def extract_lower_house_dist(self, input_dict):
        return {'LOWER_HOUSE_DIST': None}

    def extract_precinct(self, input_dict):
        return {
            'PRECINCT': input_dict['Precinct'],
            'PRECINCT_SPLIT': input_dict['Precinct']
        }

    def extract_county_board_dist(self, input_dict):
        return {'COUNTY_BOARD_DIST': input_dict['CountyComm']}

    def extract_school_board_dist(self, input_dict):
        return {'SCHOOL_BOARD_DIST': input_dict['School']}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

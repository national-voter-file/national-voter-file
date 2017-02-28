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

default_file = 'co_sample.csv'


class StatePreparer(BasePreparer):
    state_path = 'co'
    state_name = 'Colorado'
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

    co_party_map = {
        "DEM": "DEM",
        "REP": "REP",
        "LBR": "LIB",
        "GRN": "GRN",
        "ACN": "AMC",
        "UNI": "UTY",
        "UAF": "UN"
    }

    co_gender_map = {
        'Female': 'F',
        'Male': 'M'
    }

    col_map = {
        'TITLE': None,
        'FIRST_NAME': 'FIRST_NAME',
        'MIDDLE_NAME': 'MIDDLE_NAME',
        'LAST_NAME': 'LAST_NAME',
        'NAME_SUFFIX': 'NAME_SUFFIX',
        'RACE': None,
        'BIRTH_STATE': None,
        'LANGUAGE_CHOICE': None,
        'EMAIL': None,
        'PHONE': 'PHONE_NUM',
        'DO_NOT_CALL_STATUS': None,
        'COUNTYCODE': 'COUNTY_CODE',
        'COUNTY_VOTER_REF': None,
        'PARTY': None,
        'CONGRESSIONAL_DIST': 'CONGRESSIONAL_DIST',
        'UPPER_HOUSE_DIST': 'UPPER_HOUSE_DIST',
        'LOWER_HOUSE_DIST': 'LOWER_HOUSE_DIST',
        'PRECINCT': 'PRECINCT',
        'COUNTY_BOARD_DIST': None,
        'SCHOOL_BOARD_DIST': None,
        'PRECINCT_SPLIT': 'SPLIT',
        'REGISTRATION_STATUS': 'STATUS_CODE',
        'ABSENTEE_TYPE': None
    }

    #### Demographics methods ##################################################

    def extract_gender(self, input_dict):
        return {'GENDER': self.co_gender_map.get(input_dict['GENDER'], None)}

    def extract_birthdate(self, input_dict):
        # TODO: Putting Jan 1 of birth year, need to figure out how to handle
        return {
            'BIRTHDATE': date(int(input_dict['BIRTH_YEAR']), 1, 1),
            'BIRTHDATE_IS_ESTIMATE':'Y'
        }

    #### Address methods #######################################################

    def extract_registration_address(self, input_dict):
        # TODO: Currently parsing with usaddress, but CO has almost all fields,
        # might be worth just taking as is
        address_str = ' '.join([
            input_dict['HOUSE_NUM'],
            input_dict['HOUSE_SUFFIX'],
            input_dict['PRE_DIR'],
            input_dict['STREET_NAME'],
            input_dict['STREET_TYPE'],
            input_dict['POST_DIR'],
            input_dict['UNIT_TYPE'],
            input_dict['UNIT_NUM']
        ])

        raw_dict = {
            'RAW_ADDR1': address_str,
            # Including Raw Addr 2 as same because not as clear of a division
            'RAW_ADDR2': address_str,
            'RAW_CITY': input_dict['RESIDENTIAL_CITY'],
            'RAW_ZIP': input_dict['RESIDENTIAL_ZIP_CODE']
        }

        usaddress_dict = self.usaddress_tag(address_str)[0]

        converted_addr = self.convert_usaddress_dict(usaddress_dict)

        converted_addr.update({
            'PLACE_NAME': input_dict['RESIDENTIAL_CITY'],
            'STATE_NAME': input_dict['RESIDENTIAL_STATE'],
            'ZIP_CODE': input_dict['RESIDENTIAL_ZIP_CODE'],
            'VALIDATION_STATUS': '2'
        })

        converted_addr.update(raw_dict)

        return converted_addr

    def extract_mailing_address(self, input_dict):
        if input_dict['MAIL_ADDR1'].strip():
            try:
                tagged_address, address_type = usaddress.tag(' '.join([
                    input_dict['MAIL_ADDR1'],
                    input_dict['MAIL_ADDR2'],
                    input_dict['MAIL_ADDR3']
                ]))

                if address_type == 'Ambiguous':
                    print("Warn - {}: Ambiguous mailing address falling back to residential ({})".format(address_type, input_dict['MAIL_ADDR1']))
                    tagged_address = {}

                if len(tagged_address) > 0:
                    return {
                        'MAIL_ADDRESS_LINE1': self.construct_mail_address_1(
                            tagged_address,
                            address_type,
                        ),
                        'MAIL_ADDRESS_LINE2': self.construct_mail_address_2(tagged_address),
                        'MAIL_CITY': tagged_address['PlaceName'] if 'PlaceName' in tagged_address else "",
                        'MAIL_ZIP_CODE': tagged_address['ZipCode'] if 'ZipCode' in tagged_address else "",
                        'MAIL_STATE': tagged_address['StateName'] if 'StateName' in tagged_address else "",
                        'MAIL_COUNTRY': ""
                    }
                else:
                    return {}
            except usaddress.RepeatedLabelError as e:
                print('Warn: Can\'t parse mailing address. Falling back to residential ({})'.format(e.parsed_string))
                return {}
        else:
            return {}

    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF' : 'CO' + input_dict['VOTER_ID']}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE': self.convert_date(input_dict['REGISTRATION_DATE'])}

    def extract_party(self, input_dict):
        return {'PARTY': self.co_party_map[input_dict['PARTY']]}

    def extract_congressional_dist(self, input_dict):
        # Starts with 14 chars of "Congressional ", skipping
        return {'CONGRESSIONAL_DIST': input_dict['CONGRESSIONAL'][14:]}

    def extract_upper_house_dist(self, input_dict):
        # Starts with 13 chars of "State Senate ", skipping
        return {'UPPER_HOUSE_DIST': input_dict['STATE_SENATE'][13:]}

    def extract_lower_house_dist(self, input_dict):
        # Starts with 12 chars of "State House ", skipping
        return {'LOWER_HOUSE_DIST': input_dict['STATE_HOUSE'][12:]}


if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

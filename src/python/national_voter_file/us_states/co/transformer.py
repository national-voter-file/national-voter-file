import csv
import os
import re
import sys

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
from datetime import date
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'co_sample.csv'


class StatePreparer(BasePreparer):
    state_path = 'co'
    state_name='Colorado'
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
    input_fields = None #The file has headers

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

    #### Contact methods #######################################################

    def extract_name(self, input_dict):
        return {
            'TITLE': None,
            'FIRST_NAME': input_dict['FIRST_NAME'],
            'MIDDLE_NAME': input_dict['MIDDLE_NAME'],
            'LAST_NAME': input_dict['LAST_NAME'],
            'NAME_SUFFIX': input_dict['NAME_SUFFIX'],
        }

    extract_email = lambda self, i: {'EMAIL': None}

    def extract_phone_number(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PHONE'
        """
        return {'PHONE': input_dict['PHONE_NUM']}

    extract_do_not_call_status = lambda self, i: {'DO_NOT_CALL_STATUS': None}

    #### Demographics methods ##################################################

    def extract_gender(self, input_dict):
        return {'GENDER': self.co_gender_map.get(input_dict['GENDER'], None)}

    extract_race = lambda self, i: {'RACE': None}

    extract_birth_state = lambda self, i: {'BIRTH_STATE': None}

    def extract_birthdate(self, input_dict):
        # TODO: Putting Jan 1 of birth year, need to figure out how to handle
        return {
            'BIRTHDATE': date(int(input_dict['BIRTH_YEAR']), 1, 1),
            'BIRTHDATE_IS_ESTIMATE':'Y'
        }

    extract_language_choice = lambda self, i: {'LANGUAGE_CHOICE': None}

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

        for r in ['RAW_ADDR1', 'RAW_ADDR2']:
            if not raw_dict[r].strip():
                raw_dict[r] = '--Not provided--'

        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)

        converted_addr = self.convert_usaddress_dict(usaddress_dict)

        converted_addr.update({
            'PLACE_NAME': input_dict['RESIDENTIAL_CITY'],
            'STATE_NAME': input_dict['RESIDENTIAL_STATE'],
            'ZIP_CODE': input_dict['RESIDENTIAL_ZIP_CODE'],
            'VALIDATION_STATUS': '2'
        })

        converted_addr.update(raw_dict)

        return converted_addr

    def extract_county_code(self, input_dict):
        return {'COUNTYCODE' : input_dict['COUNTY_CODE']}

    def extract_mailing_address(self, input_dict):
        """

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

        if input_dict['MAIL_ADDR1'].strip():
            try:
                tagged_address, address_type = usaddress.tag(' '.join([
                input_dict['MAIL_ADDR1'],
                input_dict['MAIL_ADDR2'],
                input_dict['MAIL_ADDR3']]))

                if( address_type == 'Ambiguous'):
                    print("Warn - %s: Ambiguous mailing address falling back to residential (%s)" % (address_type, input_dict['MAIL_ADDR1']))
                    tagged_address = {}

                if(len(tagged_address) > 0):
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
            except usaddress.RepeatedLabelError as e :
                print('Warn: Can\'t parse mailing address. Falling back to residential (%s)' % (e.parsed_string))
                return {}
        else:
            return {}

    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF' : input_dict['VOTER_ID']}

    extract_county_voter_ref = lambda self, i: {'COUNTY_VOTER_REF': None}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE': self.convert_date(input_dict['REGISTRATION_DATE'])}

    def extract_registration_status(self, input_dict):
        return {'REGISTRATION_STATUS' : input_dict['STATUS_CODE']}

    extract_absentee_type = lambda self, i: {'ABSENTEE_TYPE': None}

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

    extract_precinct = BaseTransformer.map_extract_by_keys('PRECINCT')

    extract_county_board_dist = lambda self, i: {'COUNTY_BOARD_DIST': None}

    extract_school_board_dist = lambda self, i: {'SCHOOL_BOARD_DIST': None}

    def extract_precinct(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT'
        """
        return {'PRECINCT' : input_dict['PRECINCT'],
                'PRECINCT_SPLIT' : input_dict['SPLIT']}


if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

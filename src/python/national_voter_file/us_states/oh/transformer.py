import csv
import os
import re
import sys
from datetime import datetime

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'SWVF_1_44_SAMPLE.csv'

class StatePreparer(BasePreparer):
    state_path = 'oh'
    state_name = 'Ohio'
    sep = ','

    def __init__(self, input_path, *args, **kwargs):
        super(StatePreparer, self).__init__(input_path, *args, **kwargs)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        if not self.history:
            reader = self.dict_iterator(self.open(self.input_path))
            for row in reader:
                yield row
        else:
            hist_iter = self.history_iterator(self.input_path)
            for row in hist_iter:
                yield row

    def history_iterator(self, input_path):
        with open(input_path, 'r') as f:
            reader = csv.reader(f)
            header_row = next(reader)
        elec_cols = [c for c in header_row if re.match(r'\w{5,7}-\d{2}\/\d{2}\/\d{4}', c)]
        fieldnames = ['SOS_VOTERID'] + elec_cols

        reader = csv.DictReader(
            input_path, delimiter=self.sep, fieldnames=fieldnames
        )
        for row in reader:
            for c in elec_cols:
                elec_split = c.split('-')
                yield {
                    'SOS_VOTERID': row['SOS_VOTERID'],
                    'ELECTION_DATE': elec_split[1],
                    'ELECTION_TYPE': elec_split[0],
                    'VOTE_METHOD': row[c]
                }


class StateTransformer(BaseTransformer):
    date_format = '%Y-%m-%d'
    input_fields = None

    ohio_party_map = {
        "C": "AMC",
        "D": "DEM",
        "G": "GRN",
        "L": "LIB",
        "N": "NLP",
        "R": "REP",
        "S": "SP",
        " ": "UN",
        "":  "UN"
    }

    #### Contact methods #######################################################

    def extract_name(self, input_dict):
        return {
            'TITLE': None,
            'FIRST_NAME': input_dict['FIRST_NAME'],
            'MIDDLE_NAME': input_dict['MIDDLE_NAME'],
            'LAST_NAME': input_dict['LAST_NAME'],
            'NAME_SUFFIX': input_dict['SUFFIX'],
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
        return {
            'BIRTHDATE': self.convert_date(input_dict['DATE_OF_BIRTH']),
            'BIRTHDATE_IS_ESTIMATE': 'N'
        }

    def extract_language_choice(self, input_dict):
        return {'LANGUAGE_CHOICE': None}

    #### Address methods #######################################################

    def extract_registration_address(self, input_dict):
        address_components = [
            'RESIDENTIAL_ADDRESS1',
            'RESIDENTIAL_SECONDARY_ADDR'
        ]
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])

        raw_dict = {
            'RAW_ADDR1': input_dict['RESIDENTIAL_ADDRESS1'],
            'RAW_ADDR2': input_dict['RESIDENTIAL_SECONDARY_ADDR'],
            'RAW_CITY': input_dict['RESIDENTIAL_CITY'],
            'RAW_ZIP': input_dict['RESIDENTIAL_ZIP']
        }

        usaddress_dict = self.usaddress_tag(address_str)[0]

        if usaddress_dict:
            converted_addr = self.convert_usaddress_dict(usaddress_dict)

            converted_addr.update({
                'PLACE_NAME': input_dict['RESIDENTIAL_CITY'],
                'STATE_NAME': input_dict['RESIDENTIAL_STATE'],
                'ZIP_CODE': input_dict['RESIDENTIAL_ZIP'],
                'VALIDATION_STATUS': '2'
            })
            converted_addr.update(raw_dict)
        else:
            converted_addr = self.constructEmptyResidentialAddress()
            converted_addr.update(raw_dict)
            converted_addr.update({
                'STATE_NAME': input_dict['RESIDENTIAL_STATE'],
                'VALIDATION_STATUS': '1'
            })
        return converted_addr

    def extract_county_code(self, input_dict):
        return {'COUNTYCODE': input_dict['COUNTY_NUMBER']}

    def extract_mailing_address(self, input_dict):
        if input_dict['MAILING_ADDRESS1'].strip() and input_dict['MAILING_CITY'].strip():
            return {
                'MAIL_ADDRESS_LINE1': input_dict['MAILING_ADDRESS1'],
                'MAIL_ADDRESS_LINE2': input_dict['MAILING_SECONDARY_ADDRESS'],
                'MAIL_CITY': input_dict['MAILING_CITY'],
                'MAIL_STATE': input_dict['MAILING_STATE'],
                'MAIL_ZIP_CODE': input_dict['MAILING_ZIP'],
                'MAIL_COUNTRY': input_dict['MAILING_COUNTRY'] if input_dict['MAILING_COUNTRY'] else "USA"
            }
        else:
            return {}


    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF': input_dict['SOS_VOTERID']}

    def extract_county_voter_ref(self, input_dict):
        return {'COUNTY_VOTER_REF': input_dict['COUNTY_ID']}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE': self.convert_date(input_dict['REGISTRATION_DATE'])}

    def extract_registration_status(self, input_dict):
        return {'REGISTRATION_STATUS': input_dict['VOTER_STATUS']}

    def extract_absentee_type(self, input_dict):
        return {'ABSENTEE_TYPE': None}

    def extract_party(self, input_dict):
        return {'PARTY': self.ohio_party_map[input_dict['PARTY_AFFILIATION']]}

    def extract_congressional_dist(self, input_dict):
        return {'CONGRESSIONAL_DIST': input_dict['CONGRESSIONAL_DISTRICT']}

    def extract_upper_house_dist(self, input_dict):
        return {'UPPER_HOUSE_DIST': input_dict['STATE_SENATE_DISTRICT']}

    def extract_lower_house_dist(self, input_dict):
        return {'LOWER_HOUSE_DIST': input_dict['STATE_REPRESENTATIVE_DISTRICT']}

    def extract_precinct(self, input_dict):
        return {'PRECINCT': input_dict['PRECINCT_CODE']}

    def extract_county_board_dist(self, input_dict):
        # TODO: Not sure if mapping exists, verify
        return {'COUNTY_BOARD_DIST': None}

    def extract_school_board_dist(self, input_dict):
        # Several different types of school districts, but seem mutually exclusive
        # Using whichever has a value, but defaulting to None
        school_board_dist = None

        if len(input_dict['CITY_SCHOOL_DISTRICT'].strip()) > 0:
            school_board_dist = input_dict['CITY_SCHOOL_DISTRICT'].strip()
        elif len(input_dict['EXEMPTED_VILL_SCHOOL_DISTRICT'].strip()) > 0:
            school_board_dist = input_dict['EXEMPTED_VILL_SCHOOL_DISTRICT'].strip()
        elif len(input_dict['LOCAL_SCHOOL_DISTRICT'].strip()) > 0:
            school_board_dist = input_dict['LOCAL_SCHOOL_DISTRICT'].strip()

        return {'SCHOOL_BOARD_DIST': school_board_dist}

    def extract_precinct_split(self, input_dict):
        return {'PRECINCT_SPLIT': None}

    # HISTORY METHODS
    hist_state_voter_ref = extract_state_voter_ref

    def hist_election_info(self, input_dict):
        return {
            'ELECTION_DATE': datetime.strptime(
                input_dict['ELECTION_DATE'], '%m/%d/%Y'
            ).date(),
            'ELECTION_TYPE': input_dict['ELECTION_TYPE'],
            'VOTE_METHOD': input_dict['VOTE_METHOD']
        }


if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

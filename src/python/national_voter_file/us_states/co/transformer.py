import csv
import os
import re
import sys
import gzip
from zipfile import ZipFile
from io import BytesIO, TextIOWrapper
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
    sep = ','

    voter_pre = 'Registered_Voters_List'
    hist_pre = 'Master_Voting_History_List'

    def __init__(self, input_path, *args, **kwargs):
        super(StatePreparer, self).__init__(input_path, *args, **kwargs)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        if self.input_path.endswith('.zip'):
            z = ZipFile(self.input_path)
        else:
            return self.yield_rows()

        if self.history:
            return self.yield_hist_rows(z)
        else:
            return self.yield_zip_rows(z)

    def yield_rows(self):
        reader = self.dict_iterator(self.open(self.input_path))
        for row in reader:
            yield row

    def yield_zip_rows(self, zip_obj):
        prefix = self.voter_pre
        file_list = [f for f in zip_obj.namelist() if prefix in f and f.endswith('.zip')]

        for f in file_list:
            z_data = ZipFile(BytesIO(zip_obj.read(f)))
            for z_f in z_data.namelist():
                with z_data.open(z_f) as zdf:
                    reader = csv.DictReader(TextIOWrapper(zdf), delimiter=self.sep)
                    for row in reader:
                        yield row

    def yield_hist_rows(self, zip_obj):
        prefix = self.hist_pre
        file_list = [f for f in zip_obj.namelist() if prefix in f and f.endswith('.gz')]

        for f in file_list:
            with gzip.open(BytesIO(zip_obj.read(f)), 'rt') as gf:
                reader = csv.DictReader(gf, delimiter=self.sep)
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

        if usaddress_dict:
            converted_addr = self.convert_usaddress_dict(usaddress_dict)
            converted_addr.update(raw_dict)
            converted_addr.update({
                'PLACE_NAME': input_dict['RESIDENTIAL_CITY'],
                'STATE_NAME': input_dict['RESIDENTIAL_STATE'],
                'ZIP_CODE': input_dict['RESIDENTIAL_ZIP_CODE'],
                'VALIDATION_STATUS': '2'
            })
        else:
            converted_addr = self.constructEmptyResidentialAddress()
            converted_addr.update(raw_dict)
            converted_addr.update({
                'STATE_NAME': input_dict['RESIDENTIAL_STATE'],
                'VALIDATION_STATUS': '1'
            })

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

    hist_state_voter_ref = extract_state_voter_ref

    def hist_election_info(self, input_dict):
        return {
            'ELECTION_DATE': self.convert_date(input_dict['ELECTION_DATE']),
            'ELECTION_TYPE': input_dict['ELECTION_TYPE'],
            # TODO: Need to standardize vote method codes
            'VOTE_METHOD': input_dict['VOTING_METHOD']
        }


if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

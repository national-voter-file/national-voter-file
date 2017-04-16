import csv
import os
import re
import sys

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'AllNYSVoters20160831SAMPLE.txt'

class StatePreparer(BasePreparer):
    state_path = 'ny'
    state_name = 'NewYork'
    sep = ','

    def __init__(self, input_path, *args, **kwargs):
        super(StatePreparer, self).__init__(input_path, *args, **kwargs)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        reader = self.dict_iterator(self.open(self.input_path))
        for row in reader:
            yield row

class StateTransformer(BaseTransformer):
    date_format = "%Y%m%d"

    input_fields = [
        'LASTNAME',
        'FIRSTNAME',
        'MIDDLENAME',
        'NAMESUFFIX',
        'RADDNUMBER',
        'RHALFCODE',
        'RAPARTMENT',
        'RPREDIRECTION',
        'RSTREETNAME',
        'RPOSTDIRECTION',
        'RCITY',
        'RZIP5',
        'RZIP4',
        'MAILADD1',
        'MAILADD2',
        'MAILADD3',
        'MAILADD4',
        'DOB',
        'GENDER',
        'ENROLLMENT',
        'OTHERPARTY',
        'COUNTYCODE',
        'ED',
        'LD',
        'TOWNCITY',
        'WARD',
        'CD',
        'SD',
        'AD',
        'LASTVOTEDDATE',
        'PREVYEARVOTED',
        'PREVCOUNTY',
        'PREVADDRESS',
        'PREVNAME',
        'COUNTYVRNUMBER',
        'REGDATE',
        'VRSOURCE',
        'IDREQUIRED',
        'IDMET',
        'STATUS',
        'REASONCODE',
        'INACT_DATE',
        'PURGE_DATE',
        'SBOEID',
        'VoterHistory'
    ]

    ny_party_map = {
        "DEM": "DEM",
        "REP": "REP",
        "CON": "CON",
        "GRE": "GRN",
        "REF": "REF",
        "WOR": "WOR",
        "IND": "IDP",
        "WEP": "WEP",
        "SCC": "SCC",
        "BLK": "UN",
        " ": "UN"
    }

    ny_other_party_map = {
        "LBT": "LIB",
        "SAP": "SAP",
        "FDM": "FDM",
        "GRE": "GRN",
        "RTH": "RTH",
        "SWP": "SWP",
        "TXP": "TXP",
        "APP": "APP"

    }

    #### Contact methods #######################################################

    def extract_name(self, input_dict):
        return {
            'TITLE': None,
            'FIRST_NAME': input_dict['FIRSTNAME'],
            'MIDDLE_NAME': input_dict['MIDDLENAME'],
            'LAST_NAME': input_dict['LASTNAME'],
            'NAME_SUFFIX': input_dict['NAMESUFFIX'],
        }

    def extract_email(self, input_dict):
        return {'EMAIL': None}

    def extract_phone_number(self, input_dict):
        return {'PHONE': None}

    def extract_do_not_call_status(self, input_dict):
        return {'DO_NOT_CALL_STATUS': None}

    #### Demographics methods ##################################################

    def extract_gender(self, input_dict):
        return {'GENDER': input_dict['GENDER']}

    def extract_birthdate(self, input_dict):
        return {
            'BIRTHDATE': self.convert_date(input_dict['DOB']),
            'BIRTHDATE_IS_ESTIMATE':'N'
        }

    def extract_birth_state(self, input_dict):
        return {'BIRTH_STATE': None}

    def extract_race(self, input_dict):
        return {'RACE': None}

    def extract_language_choice(self, input_dict):
        return {'LANGUAGE_CHOICE': None}

    #### Address methods #######################################################

    def extract_registration_address(self, input_dict):
        aptField = input_dict['RAPARTMENT'].strip()
        address_str = ' '.join([
            input_dict['RADDNUMBER'],
            input_dict['RHALFCODE'],
            input_dict['RPREDIRECTION'],
            input_dict['RSTREETNAME'],
            input_dict['RPOSTDIRECTION'],
            'Apt ' + input_dict['RAPARTMENT'] if aptField and aptField != 'APT' else ''
        ])

        raw_dict = {
            'RAW_ADDR1': self.construct_val(input_dict, ['RADDNUMBER', 'RHALFCODE', 'RPREDIRECTION', 'RSTREETNAME', 'RPOSTDIRECTION']),
            'RAW_ADDR2': input_dict['RAPARTMENT'].strip(),
            'RAW_CITY': input_dict['RCITY'],
            'RAW_ZIP': input_dict['RZIP5']
        }

        usaddress_dict = self.usaddress_tag(address_str)[0]
        if usaddress_dict:
            converted_addr = self.convert_usaddress_dict(usaddress_dict)

            converted_addr.update({
                'PLACE_NAME':input_dict['RCITY'],
                'STATE_NAME':"NY",
                'ZIP_CODE':input_dict['RZIP5'],
                'VALIDATION_STATUS': '2'
            })

            converted_addr.update(raw_dict)

        else:
            converted_addr = self.constructEmptyResidentialAddress()
            converted_addr.update(raw_dict)
            converted_addr.update({
                'STATE_NAME': 'NY',
                'VALIDATION_STATUS': '1'
            })

        return converted_addr

    def extract_county_code(self, input_dict):
        return {'COUNTYCODE': input_dict['COUNTYCODE']}

    def extract_mailing_address(self, input_dict):
        if input_dict['MAILADD1'].strip():
            try:
                tagged_address, address_type = usaddress.tag(' '.join([
                    input_dict['MAILADD1'],
                    input_dict['MAILADD2'],
                    input_dict['MAILADD3'],
                    input_dict['MAILADD4']
                ]))

                if address_type == 'Ambiguous':
                    print("Warn - %s: Ambiguous mailing address falling back to residential (%s)" % (address_type, input_dict['MAILADD1']))
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
                print('Warn: Can\'t parse mailing address. Falling back to residential (%s)' % e.parsed_string)
                return {}
        else:
            return {}

    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF': input_dict['SBOEID']}

    def extract_county_voter_ref(self, input_dict):
        return {'COUNTY_VOTER_REF': input_dict['COUNTYVRNUMBER']}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE': self.convert_date(input_dict['REGDATE'])}

    def extract_registration_status(self, input_dict):
        return {'REGISTRATION_STATUS': input_dict['STATUS']}

    def extract_absentee_type(self, input_dict):
        return {'ABSENTEE_TYPE': None}

    def extract_party(self, input_dict):
        party = input_dict['ENROLLMENT'].upper()
        if party != "OTH":
            party_code = self.ny_party_map[party]
        else:
            party_code = self.ny_other_party_map[input_dict['OTHERPARTY'].upper()]

        return {'PARTY': party_code}

    def extract_congressional_dist(self, input_dict):
        return {'CONGRESSIONAL_DIST': input_dict['CD']}

    def extract_upper_house_dist(self, input_dict):
        return {'UPPER_HOUSE_DIST': input_dict['SD']}

    def extract_lower_house_dist(self, input_dict):
        return {'LOWER_HOUSE_DIST': input_dict['AD']}

    def extract_precinct(self, input_dict):
        return {'PRECINCT': input_dict['ED']}

    def extract_county_board_dist(self, input_dict):
        # TODO: Not sure if mapping exists, verify
        return {'COUNTY_BOARD_DIST': None}

    def extract_school_board_dist(self, input_dict):
        # TODO: Not sure if mapping exists, verify
        return {'SCHOOL_BOARD_DIST': None}

    def extract_precinct_split(self, input_dict):
        # There is no split, so copy the precinct
        return {'PRECINCT_SPLIT': "%02d/%03d/%02d" % (int(input_dict['COUNTYCODE']), int(input_dict['ED']), int(input_dict['AD']))}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

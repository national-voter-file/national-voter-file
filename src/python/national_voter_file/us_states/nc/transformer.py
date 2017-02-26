import csv
import os
import re
import sys

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'ncvoter_StatewideSAMPLE.csv'

class StatePreparer(BasePreparer):
    state_path = 'nc'
    state_name = 'NorthCarolina'
    sep = '\t'

    def __init__(self, input_path, *args):
        super(StatePreparer, self).__init__(input_path, *args)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        reader = self.dict_iterator(self.open(self.input_path))
        for row in reader:
            yield row

class StateTransformer(BaseTransformer):
    date_format = '%m/%d/%Y'
    input_fields = None

    col_type_dict = BaseTransformer.col_type_dict.copy()
    col_type_dict['PRECINCT_SPLIT'] = set([str, type(None)])

    col_map = {
        'PRECINCT_SPLIT': None
    }

    north_carolina_party_map = {
        'DEM':'DEM',
        'REP':'REP',
        'LIB' : 'LIB',
        'UNA':'UN',
        ' ' : 'UN',
        '': 'UN'
    }

    # There are #<street type> artifacts in street address. These
    # will be ignored until more information can clarify purpose.
    hashtag_patterns = ['#RD', '#ROAD', '#DR', '#DRIVE', '#LN', '#LANE']
    hashtag_patterns += ['#WAY', '#CIRCLE', '#CIR', '#SLIP', '#BLVD', '#MAIN']
    hashtag_patterns += ['#HILL', '#HIGHWAY']

    #### Contact methods #######################################################

    def extract_name(self, input_dict):
        first_name = input_dict['first_name']
        #TODO: Use standard value for none
        if first_name is None or first_name == '':
            first_name = 'none'
        return {
            'TITLE': input_dict['name_prefx_cd'],
            'FIRST_NAME': first_name,
            'MIDDLE_NAME': input_dict['middle_name'],
            'LAST_NAME': input_dict['last_name'],
            'NAME_SUFFIX': input_dict['name_suffix_lbl'],
        }

    def extract_email(self, input_dict):
        return {'EMAIL' : None}

    def extract_phone_number(self, input_dict):
        # TODO: Add parenthesis to area code?
        return {'PHONE' : input_dict['full_phone_number']}

    def extract_do_not_call_status(self, input_dict):
        return {'DO_NOT_CALL_STATUS' : None}

    #### Demographics methods ##################################################

    def extract_gender(self, input_dict):
        gender = input_dict['gender_code']
        if len(gender) == 0:
            gender = 'U'
        return {'GENDER' : gender}

    def extract_race(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'RACE'
        """
        return {'RACE' : input_dict['race_code']}

    def extract_birth_state(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'BIRTH_STATE'
        """
        return {'BIRTH_STATE': input_dict['birth_state']}


    def extract_birthdate(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'BIRTHDATE'
        """
        #TODO: Estimate from age/age range?
        return {'BIRTHDATE' : None,
                'BIRTHDATE_IS_ESTIMATE' : 'Yes'}

    def extract_language_choice(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data

        Outputs:
            Dictionary with following keys
                'LANGUAGE_CHOICE'
        """
        return {'LANGUAGE_CHOICE' : None}

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
            'res_street_address',
        ]
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])

        # res_street_address contains #<description> in some entries.
        # This is removed until we know we should not remove it.
        res_street_addr = input_dict['res_street_address']
        for pattern in self.hashtag_patterns:
            if pattern in res_street_addr:
                res_street_addr = ' '.join(res_street_addr.split(pattern))
            if pattern in address_str:
                address_str = ' '.join(address_str.split(pattern))

        # save the raw information too
        raw_dict = {
            'RAW_ADDR1' : res_street_addr,
            'RAW_ADDR2' : None,
            'RAW_CITY'  : input_dict['res_city_desc'],
            'RAW_ZIP'   : input_dict['zip_code']
        }

        state_name = input_dict['state_cd']
        if len(state_name.strip()) == 0:
            state_name = 'NC'

        # use the usaddress_tag method to handle errors
        usaddress_dict = self.usaddress_tag(address_str)[0]

        # use the convert_usaddress_dict to get correct column names
        # and fill in missing values
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
        return {'COUNTYCODE' : input_dict['county_id']}

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
        if input_dict['mail_addr1'].strip() and input_dict['mail_city'].strip():
            return {
                'MAIL_ADDRESS_LINE1': input_dict['mail_addr1'],
                'MAIL_ADDRESS_LINE2': " ".join([
                    input_dict['mail_addr2'],
                    input_dict['mail_addr3'],
                    input_dict['mail_addr4']]),
                'MAIL_CITY': input_dict['mail_city'],
                'MAIL_STATE': input_dict['mail_state'],
                'MAIL_ZIP_CODE': input_dict['mail_zipcode'],
                'MAIL_COUNTRY': 'USA'
            }
        else:
            return {}

    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF' : 'NC' + input_dict['voter_reg_num']}

    def extract_county_voter_ref(self, input_dict):
        return {'COUNTY_VOTER_REF' : None}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE' : self.convert_date(input_dict['registr_dt'])}

    def extract_registration_status(self, input_dict):
        return {'REGISTRATION_STATUS' : input_dict['voter_status_desc']}

    def extract_absentee_type(self, input_dict):
        return {'ABSENTEE_TYPE' : None}

    def extract_party(self, input_dict):
        return {'PARTY' : self.north_carolina_party_map[input_dict['party_cd']]}

    def extract_congressional_dist(self, input_dict):
        cong_dist = input_dict['cong_dist_abbrv']
        #TODO: Use standard value for none
        if cong_dist == ' ' or not cong_dist or len(cong_dist) == 0:
            cong_dist = 'none'
        return {'CONGRESSIONAL_DIST' : cong_dist}

    def extract_upper_house_dist(self, input_dict):
        return {'UPPER_HOUSE_DIST' : input_dict['nc_senate_abbrv']}

    def extract_lower_house_dist(self, input_dict):
        return {'LOWER_HOUSE_DIST' : input_dict['nc_house_abbrv']}

    def extract_precinct(self, input_dict):
        precinct = input_dict['precinct_abbrv']
        # TODO: Use a standard value for none
        if not precinct:
            precinct = "none"
        precinct_split = precinct
        return {'PRECINCT' : precinct,
                'PRECINCT_SPLIT' : precinct_split}

    def extract_county_board_dist(self, input_dict):
        return {'COUNTY_BOARD_DIST' : None}

    def extract_school_board_dist(self, input_dict):
        return {'SCHOOL_BOARD_DIST' : input_dict['school_dist_abbrv']}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

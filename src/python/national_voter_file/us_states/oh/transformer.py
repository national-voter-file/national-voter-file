import csv
import os
import re
import sys

from national_voter_file.transformers.base_transformer import BaseTransformer
import usaddress

class StatePreparer:

    def __init__(self, voter_in_file_path=None, output_path=None):
        from national_voter_file.transformers import DATA_DIR

        self.voter_in_file_path = voter_in_file_path \
                                   or os.path.join(DATA_DIR,
                                                   'Ohio',
                                                   'SWVF_1_44_SAMPLE.csv')
        self.output_path = output_path \
                           or os.path.join(DATA_DIR,
                                           'Ohio',
                                           'SWVF_1_44_SAMPLE_out.csv')

        self.transformer = StateTransformer(date_format='%m/%d/%Y', sep=',',
                                            input_fields=None)

    def process(self):
        self.transformer(self.voter_in_file_path, self.output_path)

class StateTransformer(BaseTransformer):


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
            'FIRST_NAME': input_dict['FIRST_NAME'],
            'MIDDLE_NAME': input_dict['MIDDLE_NAME'],
            'LAST_NAME': input_dict['LAST_NAME'],
            'NAME_SUFFIX': input_dict['SUFFIX'],
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
        return {'PHONE': None}

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
        return {'GENDER': None}

    def extract_race(self, input_dict):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'RACE'
        """
        return {'RACE': None}

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
        return {
            'BIRTHDATE': self.convert_date(input_dict['DATE_OF_BIRTH']),
            'BIRTHDATE_IS_ESTIMATE': 'N'
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

        if(not raw_dict['RAW_ADDR1'].strip()):
            raw_dict['RAW_ADDR1'] = '--Not provided--'

        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)

        if(usaddress_dict):
            converted_addr = self.convert_usaddress_dict(usaddress_dict)

            converted_addr.update({'PLACE_NAME':input_dict['RESIDENTIAL_CITY'],
                                    'STATE_NAME':input_dict['RESIDENTIAL_STATE'],
                                    'ZIP_CODE':input_dict['RESIDENTIAL_ZIP'],
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
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTYCODE'
        """
        return {'COUNTYCODE': input_dict['COUNTY_NUMBER']}

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

        if( input_dict['MAILING_ADDRESS1'].strip() and input_dict['MAILING_CITY'].strip()):
            return {
                'MAIL_ADDRESS_LINE1': input_dict['MAILING_ADDRESS1'],
                'MAIL_ADDRESS_LINE2': input_dict['MAILING_SECONDARY_ADDRESS'],
                'MAIL_CITY': input_dict['MAILING_CITY'],
                'MAIL_STATE': input_dict['MAILING_STATE'],
                'MAIL_ZIP_CODE': input_dict['MAILING_ZIP'],
                'MAIL_COUNTRY': input_dict['MAILING_COUNTRY'] if input_dict['MAILING_COUNTRY']  else "USA"
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
        return {'STATE_VOTER_REF': input_dict['SOS_VOTERID']}

    def extract_county_voter_ref(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTY_VOTER_REF'
        """
        return {'COUNTY_VOTER_REF': input_dict['COUNTY_ID']}

    def extract_registration_date(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'REGISTRATION_DATE'
        """
        date = self.convert_date(input_dict['REGISTRATION_DATE'])
        return {'REGISTRATION_DATE': date}

    def extract_registration_status(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'REGISTRATION_STATUS'
        """
        return {'REGISTRATION_STATUS': input_dict['VOTER_STATUS']}

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
        party = input_dict['PARTY_AFFILIATION']
        return {'PARTY': self.ohio_party_map[party]}

    def extract_congressional_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'CONGRESSIONAL_DIST'
        """
        return {'CONGRESSIONAL_DIST': input_dict['CONGRESSIONAL_DISTRICT']}

    def extract_upper_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'UPPER_HOUSE_DIST'
        """
        return {'UPPER_HOUSE_DIST': input_dict['STATE_SENATE_DISTRICT']}

    def extract_lower_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'LOWER_HOUSE_DIST'
        """
        return {'LOWER_HOUSE_DIST': input_dict['STATE_REPRESENTATIVE_DISTRICT']}

    def extract_precinct(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT'
        """
        return {'PRECINCT': input_dict['PRECINCT_CODE']}

    def extract_county_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTY_BOARD_DIST'
        """
        # Not sure if mapping exists, verify
        return {'COUNTY_BOARD_DIST': None}

    def extract_school_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'SCHOOL_BOARD_DIST'
        """
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
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT_SPLIT'
        """
        # No split, copying precinct
        return {'PRECINCT_SPLIT': input_dict['PRECINCT_CODE']}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

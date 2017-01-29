import csv
import os
import re
import sys

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'some_good_sample_datafile.csv'

class StatePreparer(BasePreparer):

    state_path = 'xx' # Two letter code for state
    state_name='Xxxx' # Name of state with no spaces. Use CamelCase
    sep=',' # The character used to delimit records

    def __init__(self, input_path, *args):
        super(StatePreparer, self).__init__(input_path, *args)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
            reader = self.dict_iterator(self.open(self.input_path))
            for row in reader:
                yield row

class StateTransformer(BaseTransformer):
    date_format='%m/%d/%Y' # The format used for dates
    input_fields = None # This can be a list of column names for the input file.
                        # Use None if the file has headers


    #### Contact methods #######################################################

    def extract_name(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'TITLE'
                'FIRST_NAME'
                'MIDDLE_NAME'
                'LAST_NAME'
                'NAME_SUFFIX'
        """
        raise NotImplementedError('Must implement extract_name method.')

    def extract_email(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'EMAIL'
        """
        raise NotImplementedError('Must implement extract_email method')

    def extract_phone_number(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PHONE'
        """
        raise NotImplementedError('Must implement extract_phone_number method')

    def extract_do_not_call_status(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'DO_NOT_CALL_STATUS'
        """
        raise NotImplementedError(
            'Must implement extract_do_not_call_status method'
        )

    #### Demographics methods ##################################################

    def extract_gender(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'GENDER'
        """
        raise NotImplementedError('Must implement extract_gender method')

    def extract_birthdate(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'BIRTHDATE'
        """
        raise NotImplementedError('Must implement extract_birthdate method')

    def extract_language_choice(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'LANGUAGE_CHOICE'
        """
        raise NotImplementedError(
            'Must implement extract_language_choice method'
        )

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
        raise NotImplementedError(
            'Must implement extract_registration_address method'
        )
        # columns to create address, in order
        address_components = []
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])
        # use the usaddress_tag method to handle errors
        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)
        # use the convert_usaddress_dict to get correct column names
        # and fill in missing values
        return self.convert_usaddress_dict(usaddress_dict)

    def extract_county_code(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTYCODE'
        """
        raise NotImplementedError(
            'Must implement extract_county_code method'
        )

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
        raise NotImplementedError(
            'Must implement extract_mailing_address method'
        )
        mail_str = ' '.join([x for x in columns])
        usaddress_dict, usaddress_type = self.usaddress_tag(mail_str)
        return {
            'MAIL_ADDRESS_LINE1': self.construct_mail_address_1(
                usaddress_dict,
                usaddress_type,
            ),
            'MAIL_ADDRESS_LINE2': self.construct_mail_address_2(usaddress_dict),
            'MAIL_CITY': input_dict['MAIL_CITY'],
            'MAIL_ZIP_CODE': input_dict['MAIL_ZIP'],
            'MAIL_STATE': input_dict['MAIL_STATE'],
            'MAIL_COUNTRY': input_dict['MAIL_COUNTRY'],
        }

    #### Political methods #####################################################

    def extract_state_voter_ref(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'STATE_VOTER_REF'
        """
        raise NotImplementedError(
            'Must implement extract_state_voter_ref method'
        )

    def extract_county_voter_ref(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'COUNTY_VOTER_REF'
        """
        raise NotImplementedError(
            'Must implement extract_county_voter_ref method'
        )

    def extract_registration_date(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'REGISTRATION_DATE'
        """
        raise NotImplementedError(
            'Must implement extract_registration_date method'
        )

    def extract_registration_status(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'REGISTRATION_STATUS'
        """
        raise NotImplementedError(
            'Must implement extract_registration_status method'
        )

    def extract_absentee_type(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'ABSENTEE_TYPE'
        """
        raise NotImplementedError(
            'Must implement extract_absentee_type method'
        )

    def extract_party(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PARTY'
        """
        raise NotImplementedError(
            'Must implement extract_party method'
        )

    def extract_congressional_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'CONGRESSIONAL_DIST'
        """
        raise NotImplementedError(
            'Must implement extract_congressional_dist method'
        )

    def extract_upper_house_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'UPPER_HOUSE_DIST'
        """
        raise NotImplementedError(
            'Must implement extract_upper_house_dist method'
        )

    def extract_lower_house_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'LOWER_HOUSE_DIST'
        """
        raise NotImplementedError(
            'Must implement extract_lower_house_dist method'
        )

    def extract_precinct(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PRECINCT'
        """
        raise NotImplementedError(
            'Must implement extract_precinct method'
        )

    def extract_county_board_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'COUNTY_BOARD_DIST'
        """
        raise NotImplementedError(
            'Must implement extract_county_board_dist method'
        )

    def extract_school_board_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'SCHOOL_BOARD_DIST'
        """
        raise NotImplementedError(
            'Must implement extract_school_board_dist method'
        )

    def extract_precinct_split(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PRECINCT_SPLIT'
        """
        raise NotImplementedError(
            'Must implement extract_precinct_split method'
        )

import csv
import datetime
from collections import defaultdict

import usaddress

"""
# Raw voter data -> standardized data frame

## Overview

This file contains a base class for transforming raw data into a standardized
format. Since each state will have its own column names and quirks, the model
is to subclass the BaseTransformer class to make a StateTransformer class.

BaseTransformer provides an exhaustive list of methods you need to implement,
which begin with the word "extract". You will get an error if you forget to
implement one in your subclass.

The BaseTransformer class also provides methods for ensuring that the output
from each extract method fits the correct type.

## Example usage

1) Copy the template in state_transformer_template.py
2) Create a subclass for your state, e.g. `StateTransformer(BaseTransformer)`
2) Fill in the `extract` methods in the subclass
2) Initialize: `s = StateTransformer(date_format, sep)`
3) Call the instance: `s(input_path, output_path)`

You only need to modify methods beginning with `extract`
"""

class BaseTransformer(object):
    """
    Provides helper methods and template methods for transforming raw data
    from each state to a standardized format.

    The standardized format is encoded in the class variable col_type_dict, which
    maps each required column name to the set of acceptable types. Note that
    if you wish a type to accept none, you must enter type(None).

    Class methods beginning with the word "extract" are special. Each one takes
    a single row dictionary from the input file and returns one or more columns
    with the names in col_type_dict. These extract methods should be overwritten
    in subclasses of this class.

    The validate_output_row method ensures that exactly the correct columns
    are present at that they contain variables of the types allowed in
    col_type_dict.

    If you require column-specific checks, such as ensuring that 'PARTY' is
    either 'DEM' or 'REP', add this to the limited_value_dict class variable,
    which maps column names to the acceptable values for that column. This
    is checked at the end of validate_output_row.

    """

    # Acceptable column output types
    col_type_dict = {
        'TITLE': set([str]),
        'FIRST_NAME': set([str]),
        'MIDDLE_NAME': set([str]),
        'LAST_NAME': set([str]),
        'NAME_SUFFIX': set([str]),
        'GENDER': set([str]),
        'BIRTHDATE': set([datetime.date]),
        'LANGUAGE_CHOICE': set([str, type(None)]),
        'EMAIL': set([str, type(None)]),
        'PHONE': set([str, type(None)]),
        'DO_NOT_CALL_STATUS': set([str, type(None)]),
        'ADDRESS_NUMBER': set([str, type(None)]),
        'ADDRESS_NUMBER_PREFIX': set([str, type(None)]),
        'ADDRESS_NUMBER_SUFFIX': set([str, type(None)]),
        'BUILDING_NAME': set([str, type(None)]),
        'CORNER_OF': set([str, type(None)]),
        'INTERSECTION_SEPARATOR': set([str, type(None)]),
        'LANDMARK_NAME': set([str, type(None)]),
        'NOT_ADDRESS': set([str, type(None)]),
        'OCCUPANCY_TYPE': set([str, type(None)]),
        'OCCUPANCY_IDENTIFIER': set([str, type(None)]),
        'PLACE_NAME': set([str, type(None)]),
        'STATE_NAME': set([str]),
        'STREET_NAME': set([str, type(None)]),
        'STREET_NAME_PRE_DIRECTIONAL': set([str, type(None)]),
        'STREET_NAME_PRE_MODIFIER': set([str, type(None)]),
        'STREET_NAME_PRE_TYPE': set([str, type(None)]),
        'STREET_NAME_POST_DIRECTIONAL': set([str, type(None)]),
        'STREET_NAME_POST_MODIFIER': set([str, type(None)]),
        'STREET_NAME_POST_TYPE': set([str, type(None)]),
        'SUBADDRESS_IDENTIFIER': set([str, type(None)]),
        'SUBADDRESS_TYPE': set([str, type(None)]),
        'USPS_BOX_GROUP_ID': set([str, type(None)]),
        'USPS_BOX_GROUP_TYPE': set([str, type(None)]),
        'USPS_BOX_ID': set([str, type(None)]),
        'USPS_BOX_TYPE': set([str, type(None)]),
        'ZIP_CODE': set([str]),
        'MAIL_ADDRESS_LINE1': set([str, type(None)]),
        'MAIL_ADDRESS_LINE2': set([str, type(None)]),
        'MAIL_CITY': set([str]),
        'MAIL_STATE': set([str]),
        'MAIL_ZIP_CODE': set([str]),
        'MAIL_COUNTRY': set([str]),
        'COUNTYCODE': set([str]),
        'STATE_VOTER_REF': set([str]),
        'COUNTY_VOTER_REF': set([str]),
        'REGISTRATION_DATE': set([datetime.date]),
        'REGISTRATION_STATUS': set([str]),
        'ABSENTEE_TYPE': set([str]),
        'PARTY': set([str, type(None)]),
        'CONGRESSIONAL_DIST': set([str]),
        'UPPER_HOUSE_DIST': set([str, type(None)]),
        'LOWER_HOUSE_DIST': set([str, type(None)]),
        'PRECINCT': set([str]),
        'COUNTY_BOARD_DIST': set([str, type(None)]),
        'SCHOOL_BOARD_DIST': set([str, type(None)]),
        'PRECINCT_SPLIT': set([str]),
    }

    # some columns can only have certain values
    limited_value_dict = {
        'PARTY': set(['DEM', #Democrat
                      'REP', #Republican
                      "AI", #American Independant
                      "PF", #Peace and Freedom
                      "AMC", #American Constitution
                      "GRN", #Green
                      "LIB", #Libretarian
                      "ECO", #Ecology
                      "IDP", #Independence Party
                      "PSL", #Party for socialism and Liberation
                      "REF", #Reform Party
                      "SAP", #Sapient
                      "CON", #Conservative
                      "WOR", #Working Families
                      "WEP", #Womens Equality
                      "SCC", #Stop Common Core
                      "NLP", #Natural Law
                      "SP", #Socialist
                      "UTY", #Unity
                      "OTH", #otherwise
                      "UN" #Unaffiliated
        ]),
        'GENDER': set(['M', 'F', 'U']),
    }

    usaddress_to_standard_colnames_dict = {
        'AddressNumber': 'ADDRESS_NUMBER',
        'AddressNumberPrefix': 'ADDRESS_NUMBER_PREFIX',
        'AddressNumberSuffix': 'ADDRESS_NUMBER_SUFFIX',
        'BuildingName': 'BUILDING_NAME',
        'CornerOf': 'CORNER_OF',
        'IntersectionSeparator': 'INTERSECTION_SEPARATOR',
        'LandmarkName': 'LANDMARK_NAME',
        'NotAddress': 'NOT_ADDRESS',
        'OccupancyType': 'OCCUPANCY_TYPE',
        'OccupancyIdentifier': 'OCCUPANCY_IDENTIFIER',
        'PlaceName': 'PLACE_NAME',
        'StateName': 'STATE_NAME',
        'StreetName': 'STREET_NAME',
        'StreetNamePreDirectional': 'STREET_NAME_PRE_DIRECTIONAL',
        'StreetNamePreModifier': 'STREET_NAME_PRE_MODIFIER',
        'StreetNamePreType': 'STREET_NAME_PRE_TYPE',
        'StreetNamePostDirectional': 'STREET_NAME_POST_DIRECTIONAL',
        'StreetNamePostModifier': 'STREET_NAME_POST_MODIFIER',
        'StreetNamePostType': 'STREET_NAME_POST_TYPE',
        'SubaddressIdentifier': 'SUBADDRESS_IDENTIFIER',
        'SubaddressType': 'SUBADDRESS_TYPE',
        'USPSBoxGroupID': 'USPS_BOX_GROUP_ID',
        'USPSBoxGroupType': 'USPS_BOX_GROUP_TYPE',
        'USPSBoxID': 'USPS_BOX_ID',
        'USPSBoxType': 'USPS_BOX_TYPE',
        'ZipCode': 'ZIP_CODE',
    }

    def __init__(self, date_format, sep=','):
        """
        Inputs:
            date_format: the strptime format to translate dates
            sep: separator for input files
        """
        self.date_format = date_format
        self.sep = sep

    def __call__(self, input_path, output_path):
        """
        Set paths here
        Fails if any methods aren't implemented
        Should not be overwritten in the subclass, this method enforces a
        similar check on all data created
        """
        with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
            reader = csv.DictReader(infile, delimiter=self.sep)
            writer = csv.DictWriter(
                outfile,
                fieldnames = sorted(self.col_type_dict.keys()),
            )
            writer.writeheader()
            for input_dict in reader:
                output_dict = self.process_row(input_dict)
                self.validate_output_row(output_dict) # validate here
                writer.writerow(output_dict)

    #### Row processing methods ################################################

    def process_row(self, input_dict):
        """
        Calls each class method that begins with 'extract'
        Passes input_dict as the argument to each method
        Updates output_dict with the results from each method

        Inputs:
            input_dict: A dictionary of one row from the raw data
        Outputs:
            output_dict: A dictionary containing the fields given in
                self.col_type_dict
        """
        output_dict = {}
        # Build a list of all instance methods that begin with 'extract'
        extract_funcs = [
            getattr(self, x) for x in dir(self) if x.startswith('extract')
        ]
        for func in extract_funcs:
            output_dict.update(func(input_dict))
        return output_dict

    #### Output validation methods #############################################

    def validate_output_row(self, output_dict):
        """
        Ensures
        - Output columns match those in self.col_type_dict
        - Column value types match those in self.col_type_dict

        Inputs:
            output_dict: A dictionary of format {output_column_str: value}
        Outputs:
            None
        """
        # Check to make sure correct columns are present
        correct_output_col_set = set(self.col_type_dict.keys())
        output_dict_col_set = set(output_dict.keys())

        missing_cols = correct_output_col_set - output_dict_col_set
        extra_cols = output_dict_col_set - correct_output_col_set

        if len(missing_cols) > 0 or len(extra_cols) > 0:
            error_message = (
                'Column(s) {} are required but missing.\n'
                'Column(s) {} are present but not required.').format(
                    list(missing_cols),
                    list(extra_cols),
                )
            raise ValueError(error_message)

        # check to make sure columns are of the correct type
        type_errors = []
        for colname, value in output_dict.items():
            value_type = type(value)
            acceptable_types = self.col_type_dict[colname]
            if value_type not in acceptable_types:
                type_errors.append(
                    'Column {} requires type(s) {}, found {}.'.format(
                        colname,
                        list(acceptable_types),
                        value_type,
                    )
                )
        if len(type_errors) > 0:
            error_str = '\n'.join(sorted(type_errors))
            raise TypeError(error_str)

        # check to make sure columns contain correct values
        value_errors = []
        for col, vals in self.limited_value_dict.items():
            output_value = output_dict[col]
            # if we allow None, ignore None
            if output_value is None and type(None) in self.col_type_dict[col]:
                continue
            if output_value not in vals:
                error_message = 'Column {} requires value(s) {}, found {}'.format(
                    col,
                    list(vals),
                    output_value,
                )
                value_errors.append(error_message)
        if len(value_errors) > 0:
            error_str = '\n'.join(sorted(value_errors))
            raise ValueError(error_str)

    #### Basic conversion methods ##############################################

    def convert_date(self, date_str):
        """
        Inputs:
            date_str: a string representing a date
        Outputs:
            A datetime object created according to self.date_format
        """
        return datetime.datetime.strptime(date_str, self.date_format).date()

    def usaddress_tag(self, address_str):
        """
        We get parse misses now and then. TODO: figure out how to handle
        usaddress parse errors.

        The usaddress_type almost always comes back as a "Ambiguous"
        We use a simple convention of if there's a USPSBoxID, then it's a PO Box,
        otherwise it's a Street Address

        Input:
            address_str: string of address from input file
        Output:
            Dictionary containing tagged parts of addresses
        """
        try:
            usaddress_dict, usaddress_type = usaddress.tag(address_str)
        except usaddress.RepeatedLabelError as e:
            # this will use the second occurance of each tag
            # there is probably a better rule
            usaddress_dict = {k: v for v, k in e.parsed_string}
        # if contains a PO Box ID consider it a PO Box, else a Street Address
        if 'USPSBoxID' in usaddress_dict:
            usaddress_type = 'PO Box'
        else:
            usaddress_type = 'Street Address'
        return usaddress_dict, usaddress_type

    def convert_usaddress_dict(self, usaddress_dict):
        """
        Used for extract_registration_address. We use the usaddress package to
        parse an address. This function takes the output from usaddress.tag and
        performs two operations:
        - Converts dictionary keys to our standardized format
        - Adds None for potential fields that were not used in the present
          address

        Inputs:
            usaddress_dict: A dictionary created by the usaddress.tag method
        Outputs:
            address_dict: A dictionary of form {standardized_colname: value}
        """
        address_dict = {}
        for k, v in self.usaddress_to_standard_colnames_dict.items():
            address_dict[v] = usaddress_dict.get(k, None)
        return address_dict

    def construct_mail_address_1(self, usaddress_dict, usaddress_type):
        """
        Takes the output of usaddress.tag and makes 'MAIL_ADDRESS_LINE1'

        Inputs:
            usaddress_dict: dict output from usaddress.tag
        Outputs:
            String suitable for 'MAIL_ADDRESS_LINE1'
        """
        if usaddress_type == 'Street Address':
            cols = [
                'AddressNumberPrefix',
                'AddressNumber',
                'AddressNumberSuffix',
                'StreetNamePreDirectional',
                'StreetNamePreModifier',
                'StreetNamePreType',
                'StreetName',
                'StreetNamePostType',
                'StreetNamePostModifier',
                'StreetNamePostDirectional',
            ]
        elif usaddress_type == 'PO Box':
            cols = [
                'USPSBoxType',
                'USPSBoxID',
            ]
        output_vals = [
            usaddress_dict[x] for x in cols if x in usaddress_dict
        ]
        if len(output_vals) > 0:
            return ' '.join(output_vals)
        else:
            return None

    def construct_mail_address_2(self, usaddress_dict):
        """
        Takes the output of usaddress.tag and makes 'MAIL_ADDRESS_LINE2'

        Inputs:
            usaddress_dict: dict output from usaddress.tag
        Outputs:
            String suitable for 'MAIL_ADDRESS_LINE2'
        """
        cols = ['OccupancyType', 'OccupancyIdentifier']
        output_vals = [
            usaddress_dict[x] for x in cols if x in usaddress_dict
        ]
        if len(output_vals) > 0:
            return ' '.join(output_vals)
        else:
            return None

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
        usaddress_dict = self.usaddress_tag(address_str)
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
                'ABSTENTEE_TYPE'
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

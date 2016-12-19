from base_transformer import BaseTransformer
import usaddress
from datetime import date

class COTransformer(BaseTransformer):

    """
    A few required columns in the BaseTransformer did not have values in the
    Ohio data. Not sure what the best way of updating those on a case by case
    basis is, but given how irregular some files are it might just be worth
    allowing None for all columns
    """
    col_type_dict = BaseTransformer.col_type_dict.copy()
    col_type_dict['TITLE'] = set([str, type(None)])
    col_type_dict['GENDER'] = set([str, type(None)])
    col_type_dict['RACE'] = set([str, type(None)])
    col_type_dict['BIRTH_STATE'] = set([str, type(None)])
    col_type_dict['ABSENTEE_TYPE'] = set([str, type(None)])
    col_type_dict['COUNTY_VOTER_REF'] = set([str, type(None)])

    co_party_map = {
        "DEM":"DEM",
        "REP":"REP",
        "LBR":"LIB",
        "GRN":"GRN",
        "ACN":"AMC",
        "UNI":"UTY",
        "UAF":"UN"
    }

    co_gender_map = {
        'Female': 'F',
        'Male': 'M'
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
            'NAME_SUFFIX': input_dict['NAME_SUFFIX'],
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
        return {'PHONE': input_dict['PHONE_NUM']}

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
        return {'GENDER': self.co_gender_map.get(input_dict['GENDER'], None)}

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
        # TODO: Putting Jan 1 of birth year, need to figure out how to handle
        return {
            'BIRTHDATE': date(int(input_dict['BIRTH_YEAR']), 1, 1),
            'BIRTHDATE_IS_ESTIMATE':'Y'
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

        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)

        converted_addr = self.convert_usaddress_dict(usaddress_dict)

        converted_addr.update({'PLACE_NAME':input_dict['RESIDENTIAL_CITY'],
                               'STATE_NAME':input_dict['RESIDENTIAL_STATE'],
                               'ZIP_CODE':input_dict['RESIDENTIAL_ZIP_CODE']
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
        return {'COUNTYCODE': input_dict['COUNTY_CODE']}

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
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'STATE_VOTER_REF'
        """
        return {'STATE_VOTER_REF': input_dict['VOTER_ID']}

    def extract_county_voter_ref(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTY_VOTER_REF'
        """
        return {'COUNTY_VOTER_REF': None}

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
        # TODO: Assuming this is for A/I, but if for full Active/Inactive can use STATUS
        return {'REGISTRATION_STATUS': input_dict['STATUS_CODE']}

    def extract_absentee_type(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'ABSENTEE_TYPE'
        """
        # TODO: PERMANENT_MAIL_IN_VOTER listed, but only has No's
        return {'ABSENTEE_TYPE': None}

    def extract_party(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PARTY'
        """
        return {'PARTY': self.co_party_map[input_dict['PARTY']]}

    def extract_congressional_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'CONGRESSIONAL_DIST'
        """
        # Starts with 14 chars of "Congressional ", skipping
        return {'CONGRESSIONAL_DIST': input_dict['CONGRESSIONAL'][14:]}

    def extract_upper_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'UPPER_HOUSE_DIST'
        """
        # Starts with 13 chars of "State Senate ", skipping
        return {'UPPER_HOUSE_DIST': input_dict['STATE_SENATE'][13:]}

    def extract_lower_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'LOWER_HOUSE_DIST'
        """
        # Starts with 12 chars of "State House ", skipping
        return {'LOWER_HOUSE_DIST': input_dict['STATE_HOUSE'][12:]}

    def extract_precinct(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT'
        """
        return {'PRECINCT': input_dict['PRECINCT']}

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
        # Not sure if mapping exists, verify
        return {'SCHOOL_BOARD_DIST': None}

    def extract_precinct_split(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT_SPLIT'
        """
        # Not sure if mapping exists, verify
        return {'PRECINCT_SPLIT': input_dict['SPLIT']}

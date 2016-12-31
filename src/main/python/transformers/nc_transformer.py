from src.main.python.transformers.base_transformer import BaseTransformer
import usaddress
from datetime import datetime
import datetime as dt

class NCTransformer(BaseTransformer):

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
        output_dict = {
            'TITLE': input_columns['name_prefx_cd'],
            'FIRST_NAME': input_columns['first_name'],
            'MIDDLE_NAME': input_columns['middle_name'],
            'LAST_NAME': input_columns['last_name'],
            'NAME_SUFFIX': input_columns['name_suffix_lbl'],
        }
        return output_dict

    def extract_email(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'EMAIL'
        """
        return {'EMAIL': None}

    def extract_phone_number(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PHONE'
        """
        return {'PHONE': input_columns['full_phone_number']}

    def extract_do_not_call_status(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'DO_NOT_CALL_STATUS'
        """
        return {'DO_NOT_CALL_STATUS': None}

    #### Demographics methods ##################################################

    def extract_gender(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'GENDER'
        """
        return {'GENDER': input_columns['gender_code']}

    def extract_birthdate(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'BIRTHDATE'
        """
        #We don't have birthdate, so we'll guess
        by = int(datetime.now().year) - int(input_columns['birth_age'])
        bd = dt.date(by, 1, 1)

        output_dict = {
            'BIRTHDATE': bd,
            'BIRTHDATE_IS_ESTIMATE': "Y",
        }

        return output_dict

    def extract_birth_state(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'BIRTH_STATE'
        """
        return {'BIRTH_STATE' : input_columns['birth_state']}

    def extract_language_choice(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'LANGUAGE_CHOICE'
        """
        return {'LANGUAGE_CHOICE': None}

    def extract_race(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'RACE'
        """
        return {'RACE' : input_columns['race_code']}

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
        address_components = [
            'res_street_address',
            'res_city_desc',
            'state_cd',
            'zip_code'
        ]
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])
        # use the usaddress_tag method to handle errors
        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)

        converted_addr = self.convert_usaddress_dict(usaddress_dict)

        converted_addr.update({'PLACE_NAME':input_dict['res_city_desc'],
                                'STATE_NAME':input_dict['state_cd'],
                                'ZIP_CODE':input_dict['zip_code']
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
        return {'COUNTYCODE': input_dict['county_id']}

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
        mail_str = ' '.join([
            x for x in [
                input_dict['mail_addr1'],
                input_dict['mail_addr2'],
                input_dict['mail_addr3'],
                input_dict['mail_addr4'],
                input_dict['mail_city'],
                input_dict['mail_state'],
                input_dict['mail_zipcode']
            ] if x is not None
        ])

        usaddress_dict, usaddress_type = self.usaddress_tag(mail_str)
        return {
            'MAIL_ADDRESS_LINE1': self.construct_mail_address_1(
                usaddress_dict,
                usaddress_type,
            ),
            'MAIL_ADDRESS_LINE2': self.construct_mail_address_2(usaddress_dict),
            'MAIL_CITY': input_dict['mail_city'],
            'MAIL_ZIP_CODE': input_dict['mail_zipcode'],
            'MAIL_STATE': input_dict['mail_state'],
            'MAIL_COUNTRY': 'USA',
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
        return {'STATE_VOTER_REF': input_columns['voter_reg_num']}

    def extract_county_voter_ref(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'COUNTY_VOTER_REF'
        """
        return {'COUNTY_VOTER_REF': input_columns['voter_reg_num']}

    def extract_registration_date(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'REGISTRATION_DATE'
        """
        date = self.convert_date(input_columns['registr_dt'])
        return {'REGISTRATION_DATE': date}

    def extract_registration_status(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'REGISTRATION_STATUS'
        """
        return {'REGISTRATION_STATUS': input_columns['status_cd']}

    def extract_absentee_type(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'ABSTENTEE_TYPE'
        """
        return {'ABSENTEE_TYPE': input_columns['absent_ind']}

    def extract_party(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PARTY'
        """
        if input_columns['party_cd'] == 'UNA':
                party = 'UN'
        else:
                party = input_columns['party_cd']
        return {'PARTY': party}

    def extract_congressional_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'CONGRESSIONAL_DIST'
        """
        return {'CONGRESSIONAL_DIST': input_columns['cong_dist_abbrv']}

    def extract_upper_house_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'UPPER_HOUSE_DIST'
        """
        return {'UPPER_HOUSE_DIST': input_columns['nc_senate_abbrv']}

    def extract_lower_house_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'LOWER_HOUSE_DIST'
        """
        return {'LOWER_HOUSE_DIST': input_columns['nc_house_abbrv']}

    def extract_precinct(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PRECINCT'
        """
        return {'PRECINCT': input_columns['precinct_abbrv']}

    def extract_county_board_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'COUNTY_BOARD_DIST'
        """
        return {'COUNTY_BOARD_DIST': None}

    def extract_school_board_dist(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'SCHOOL_BOARD_DIST'
        """
        return {'SCHOOL_BOARD_DIST': input_columns['school_dist_abbrv']} #I think...

    def extract_precinct_split(self, input_columns):
        """
        Inputs:
            input_columns: name or list of columns
        Outputs:
            Dictionary with following keys
                'PRECINCT_SPLIT'
        """
        return {'PRECINCT_SPLIT': ""}

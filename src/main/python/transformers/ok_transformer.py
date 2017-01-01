from src.main.python.transformers.base_transformer import BaseTransformer
import datetime
import usaddress


class OKTransformer(BaseTransformer):
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
    col_type_dict['CONGRESSIONAL_DIST'] = set([str, type(None)])
    col_type_dict['COUNTYCODE'] = set([str, type(None)])
    col_type_dict['PRECINCT_SPLIT'] = set([str, type(None)])
    # OK is different in having missing values for birth and reg dates
    col_type_dict['BIRTHDATE'] = set([datetime.date, type(None)])
    col_type_dict['REGISTRATION_DATE'] = set([datetime.date, type(None)])

    ok_party_map = {
        "DEM": "DEM",
        "REP": "REP",
        "IND": "UN",
        "LIB": "LIB",
        "AE": "AE"
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
            'FIRST_NAME': input_dict['FirstName'],
            'MIDDLE_NAME': input_dict['MiddleName'],
            'LAST_NAME': input_dict['LastName'],
            'NAME_SUFFIX': input_dict['Suffix'],
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
        if len(input_dict['DateOfBirth']) > 0:
            date = self.convert_date(input_dict['DateOfBirth'])
        else:
            date = None
        return {'BIRTHDATE': date, 'BIRTHDATE_IS_ESTIMATE': 'N'}

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
             input_dict['StreetNum'],
             input_dict['StreetDir'],
             input_dict['StreetName'],
             input_dict['StreetType'],
             input_dict['BldgNum']
        ])

        usaddress_dict, usaddress_type = self.usaddress_tag(address_str)

        converted_addr = self.convert_usaddress_dict(usaddress_dict)

        converted_addr.update({'PLACE_NAME': input_dict['City'],
                               'STATE_NAME': 'OK',
                               'ZIP_CODE': input_dict['Zip']})

        return converted_addr

    def extract_county_code(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTYCODE'
        """
        # Files are broken into county, so not in file itself, but can get
        return {'COUNTYCODE': None}

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

        if input_dict['MailStreet1'].strip():
            try:
                tagged_address, address_type = usaddress.tag(' '.join([
                    input_dict['MailStreet1'],
                    input_dict['MailStreet2'],
                    input_dict['MailCity'],
                    input_dict['MailState'],
                    input_dict['MailZip']
                ]))

                if address_type == 'Ambiguous':
                    print("Warn - %s: Ambiguous mailing address falling back to residential (%s)" % (address_type, input_dict['MailStreet1']))
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
            except usaddress.RepeatedLabelError as e:
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
        return {'STATE_VOTER_REF': input_dict['VoterID']}

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
        # Oddly a large amount of voters don't have an original reg date
        if len(input_dict['OriginalRegistration']) > 0:
            date = self.convert_date(input_dict['OriginalRegistration'])
        else:
            date = None
        return {'REGISTRATION_DATE': date}

    def extract_registration_status(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'REGISTRATION_STATUS'
        """
        return {'REGISTRATION_STATUS': input_dict['Status']}

    def extract_absentee_type(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'ABSENTEE_TYPE'
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
        return {'PARTY': self.ok_party_map[input_dict['PolitalAff']]}

    def extract_congressional_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'CONGRESSIONAL_DIST'
        """
        # Like county, contained in data structure, not in columns
        return {'CONGRESSIONAL_DIST': None}

    def extract_upper_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'UPPER_HOUSE_DIST'
        """
        return {'UPPER_HOUSE_DIST': None}

    def extract_lower_house_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'LOWER_HOUSE_DIST'
        """
        # Starts with 12 chars of "State House ", skipping
        return {'LOWER_HOUSE_DIST': None}

    def extract_precinct(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT'
        """
        return {'PRECINCT': input_dict['Precinct']}

    def extract_county_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'COUNTY_BOARD_DIST'
        """
        return {'COUNTY_BOARD_DIST': input_dict['CountyComm']}

    def extract_school_board_dist(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'SCHOOL_BOARD_DIST'
        """
        return {'SCHOOL_BOARD_DIST': input_dict['School']}

    def extract_precinct_split(self, input_dict):
        """
        Inputs:
            input_dict: dictionary of form {colname: value} from raw data
        Outputs:
            Dictionary with following keys
                'PRECINCT_SPLIT'
        """
        # Defaulting to Precint, split doesn't exist
        return {'PRECINCT_SPLIT': input_dict['Precinct']}

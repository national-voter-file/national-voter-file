import csv
import os
import re
import sys
from datetime import date

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'nj.csv'


class StatePreparer(BasePreparer):
    state_path = 'nj'
    state_name = 'NewJersey'
    sep = '|'


    def __init__(self, input_path, *args):
        super(StatePreparer, self).__init__(input_path, *args)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        reader = self.dict_iterator(self.open(self.input_path))
        for row in reader:
            yield row

class StateTransformer(BaseTransformer):
    date_format = "%m/%d/%Y"

    nj_county_map = {
        'ATLANTIC'      : '1',
        'BERGEN'        : '3',
        'BURLINGTON'    : '5',
        'CAMDEN'        : '7',
        'CAPE MAY'      : '9',
        'CUMBERLAND'    : '11',
        'ESSEX'         : '13',
        'GLOUCESTER'    : '15',
        'HUDSON'        : '17',
        'HUNTERDON'     : '19',
        'MERCER'        : '21',
        'MIDDLESEX'     : '23',
        'MONMOUTH'      : '25',
        'MORRIS'        : '27',
        'OCEAN'         : '29',
        'PASSAIC'       : '31',
        'SALEM'         : '33',
        'SOMERSET'      : '35',
        'SUSSEX'        : '37',
        'UNION'         : '39',
        'WARREN'        : '41'
    }

    nj_party_map = {
        'DEM'   : 'DEM',
        'CNV'   : 'CON',
        'CON'   : 'CP',
        'GRE'   : 'GRN',
        'REP'   : 'REP',
        'NAT'   : 'NLP',
        'LIB'   : 'LIB',
        'RFP'   : 'REF',
        'SSP'   : 'SP',
        'UNA'   : 'UN',
        ' '     : 'UN'
    }

    col_map = {
        'TITLE': None,
        'FIRST_NAME': 'FIRST NAME',
        'MIDDLE_NAME': 'MIDDLE NAME',
        'LAST_NAME': 'LAST NAME',
        'NAME_SUFFIX': 'SUFFIX',
        'RACE': None,
        'GENDER' : None,
        'BIRTH_STATE': None,
        'LANGUAGE_CHOICE': None,
        'EMAIL': None,
        'PHONE': None,
        'DO_NOT_CALL_STATUS': None,
        'COUNTY_VOTER_REF': None,
        'CONGRESSIONAL_DIST': 'CONGRESSIONAL',
        'UPPER_HOUSE_DIST': 'LEGISLATIVE',
        'LOWER_HOUSE_DIST': 'LEGISLATIVE',
        'ABSENTEE_TYPE': None,
        'COUNTY_BOARD_DIST': 'FREEHOLDER',
        'PRECINCT_SPLIT': None,
        'REGISTRATION_STATUS': 'STATUS'
    }

    input_fields = [
        'COUNTY',
        'VOTER ID',
        'LEGACY ID',
        'LAST NAME',
        'FIRST NAME',
        'MIDDLE NAME',
        'SUFFIX',
        'STREET NUMBER',
        'SUFF A',
        'SUFF B',
        'STREET NAME',
        'APT/UNIT NO',
        'CITY',
        'MUNICIPALITY',
        'ZIP',
        'DOB',
        'PARTY CODE',
        'WARD',
        'DISTRICT',
        'STATUS',
        'CONGRESSIONAL',
        'LEGISLATIVE',
        'FREEHOLDER',
        'SCHOOL',
        'REGIONAL SCHOOL',
        'FIRE'
    ]

    #### Demographics methods #################################################

    def extract_birthdate(self, input_dict):
        dob = input_dict['DOB']
        if dob:
            dob = self.convert_date(dob)
        return {
            'BIRTHDATE': dob,
            'BIRTHDATE_IS_ESTIMATE': 'N'
        }

    #### Address methods ######################################################

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
            'STREET NUMBER',
            'SUFF A',
            'SUFF B',
            'STREET NAME',
            'APT/UNIT NO'
        ]
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])

        raw_dict = {
            'RAW_ADDR1': address_str,
            # Including Raw Addr 2 as same because not as clear of a division
            'RAW_ADDR2': address_str,
            'RAW_CITY': input_dict['CITY'],
            'RAW_ZIP': input_dict['ZIP']
        }

        usaddress_dict = self.usaddress_tag(address_str)[0]

        if usaddress_dict:
            converted_addr = self.convert_usaddress_dict(usaddress_dict)

            converted_addr.update({
                'PLACE_NAME': input_dict['CITY'],
                'STATE_NAME': 'NJ',
                'ZIP_CODE': input_dict['ZIP'],
                'VALIDATION_STATUS': '2'
            })
            converted_addr.update(raw_dict)
        else:
            converted_addr = self.constructEmptyResidentialAddress()
            converted_addr.update(raw_dict)
            converted_addr.update({
                'STATE_NAME': 'NJ',
                'VALIDATION_STATUS': '1'
            })

        return converted_addr

    def extract_mailing_address(self, input_dict):
        # columns to create address, in order
        address_components = [
            'STREET NUMBER',
            'SUFF A',
            'SUFF B',
            'STREET NAME',
            'APT/UNIT NO'
        ]
        # create address string for usaddress.tag
        address_str = ' '.join([
            input_dict[x] for x in address_components if input_dict[x] is not None
        ])
        mail_addr_dict = {}
        if address_str.strip():
            usaddress_dict, usaddress_type = self.usaddress_tag(address_str)
            if usaddress_type == 'Ambiguous':
                print('Warn - {}: Ambiguous mailing address, falling back to residential'.format(usaddress_type))
            # Making sure the usaddress_dict is not None
            elif usaddress_dict:
                mail_addr_dict = {
                    'MAIL_ADDRESS_LINE1': self.construct_mail_address_1(
                        usaddress_dict, usaddress_type
                    ),
                    'MAIL_ADDRESS_LINE2': self.construct_mail_address_2(usaddress_dict),
                    'MAIL_CITY': usaddress_dict.get('PlaceName', None),
                    'MAIL_ZIP_CODE': usaddress_dict.get('ZipCode', None),
                    'MAIL_STATE': 'NJ',
                    'MAIL_COUNTRY': 'USA'
                }
            else:
                mail_addr_dict = {}
        return mail_addr_dict

    #### Political methods ####################################################
    def extract_county_code(self, input_dict):
        return {'COUNTYCODE' : self.nj_county_map[input_dict['COUNTY']]}

    def extract_party(self, input_dict):
        return {'PARTY' : self.nj_party_map[input_dict['PARTY CODE']]}

    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF' : 'NJ' + input_dict['VOTER ID']}

    #TODO: Change format?
    def extract_precinct(self, input_dict):
        return {'PRECINCT' : input_dict['WARD'] + '-' + input_dict['DISTRICT']}

    def extract_school_board_dist(self, input_dict):
        school_dist = input_dict['SCHOOL']
        if school_dist == ' ' or school_dist == '' or school_dist is None:
            school_dist = input_dict['REGIONAL SCHOOL']
        return {'SCHOOL_BOARD_DIST': school_dist}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE' : None}

if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

import csv
import os
import re
import sys
from zipfile import ZipFile
from datetime import date

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
import usaddress

__all__ = ['default_file', 'StatePreparer', 'StateTransformer']

default_file = 'mi_sample.csv'


class StatePreparer(BasePreparer):
    state_path = 'mi'
    state_name = 'Michigan'
    sep = ','

    """
    Michigan's voter file has a strange layout which defines columns by their
    starting index and length in the table with no actual delimiters. Some columns
    seem to flow into each other. There's an intermediary step here that creates
    a CSV that is parseable by the StateTransformer directly from the zip file,
    based off of the column string indices as described in the Michigan voter file
    documentation (offset by one because their indices start at 1).
    """

    col_indices = (
        (0, 35),
        (35, 55),
        (55, 75),
        (75, 78),
        (78, 82),
        (82, 83),
        (83, 91),
        (91, 92),
        (92, 99),
        (99, 103),
        (103, 105),
        (105, 135),
        (135, 141),
        (141, 143),
        (143, 156),
        (155, 191),
        (191, 193),
        (193, 198),
        (198, 248),
        (248, 298),
        (298, 348),
        (348, 398),
        (398, 448),
        (448, 461),
        (461, 463),
        (463, 468),
        (468, 474),
        (474, 479),
        (479, 484),
        (484, 489),
        (489, 494),
        (494, 499),
        (499, 504),
        (504, 510),
        (510, 516),
        (516, 517),
        (517, 519),
        (519, 520)
    )

    election_indices = (
        (0, 13),
        (13, 21),
        (21, 71)
    )

    history_indices = (
        (0, 13),
        (13, 15),
        (15, 20),
        (20, 25),
        (25, 38),
        (38, 39)
    )

    # Has separate fields list because of intermediary loading from zip
    history_fields = [
        'STATE_VOTER_REF',
        'COUNTYCODE',
        'JURISDICTION',
        'SCHOOL_CODE',
        'ELECTION_CODE',
        'ABSENTEE_TYPE'
    ]

    def __init__(self, input_path, *args, **kwargs):
        super(StatePreparer, self).__init__(input_path, *args, **kwargs)

        if not self.transformer:
            self.transformer = StateTransformer()

    def process(self):
        """
        Process zip into separate files and use separate history generator for
        processing vote history, otherwise use simple row generator
        """
        if self.input_path.endswith('.zip'):
            z = ZipFile(self.input_path)
        else:
            return self.yield_rows(self.input_path)

        if self.history:
            return self.yield_history_rows('entire_state_h.lst', zip_obj=z)
        else:
            return self.yield_zip_rows(z, 'entire_state_v.lst')

    def yield_rows(self, input_path):
        reader = csv.DictReader(self.open(input_path), delimiter=self.sep)
        for row in reader:
            yield row

    def yield_zip_rows(self, zip_obj, input_path):
        with zip_obj.open(input_path, 'r') as infile:
            for row in infile:
                # Use column indices to split rows, yield dict in row format
                yield dict(zip(
                    self.transformer.input_fields,
                    [row[slice(*c)].strip().decode('utf-8') for c in self.col_indices]
                ))

    def yield_history_rows(self, input_path, zip_obj=None, elec_code_file=None):
        # For processing zip and raw .lst files
        if not elec_code_file:
            elec_code_file = os.path.join(DATA_DIR, 'Michigan', 'electionscd.lst')
        # Create mapping of election codes and values
        ec_map = {}
        ei = self.election_indices
        with open(elec_code_file, 'r') as ec:
            for row in ec:
                ec_map[row[slice(*ei[0])].strip()] = {
                    'ELECTION_DATE': row[slice(*ei[1])],
                    'ELECTION_TYPE': row[slice(*ei[2])]
                }

        if zip_obj is not None:
            open_f = zip_obj.open
        else:
            open_f = open

        with open_f(input_path, 'r') as infile:
            for row in infile:
                row_vals = []
                for c in self.history_indices:
                    val = row[slice(*c)].strip()
                    if hasattr(val, 'decode'):
                        val = val.decode('utf-8')
                    row_vals.append(val)

                hist_dict = dict(zip(self.history_fields, row_vals))
                el_vals = ec_map[hist_dict.pop('ELECTION_CODE')]
                hist_dict.update(el_vals)
                yield hist_dict


class StateTransformer(BaseTransformer):
    date_format = "%m%d%Y"

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
        'PHONE': None,
        'DO_NOT_CALL_STATUS': None,
        'COUNTYCODE': 'COUNTYCODE',
        'COUNTY_VOTER_REF': None,
        'PARTY': None,
        'CONGRESSIONAL_DIST': 'CONGRESSIONAL_DIST',
        'UPPER_HOUSE_DIST': 'UPPER_HOUSE_DIST',
        'LOWER_HOUSE_DIST': 'LOWER_HOUSE_DIST',
        'PRECINCT': 'WARD_PRECINCT',
        'COUNTY_BOARD_DIST': 'COUNTY_BOARD_DIST',
        'PRECINCT_SPLIT': None,
        'REGISTRATION_STATUS': 'REGISTRATION_STATUS'
    }

    input_fields = [
        'LAST_NAME',
        'FIRST_NAME',
        'MIDDLE_NAME',
        'NAME_SUFFIX',
        'BIRTH_YEAR', # YYYY
        'GENDER', # M or F
        'DATE_OF_REGISTRATION', # MMDDYYY
        'HOUSE_NUM_CHARACTER', # Alpha prefix to house num
        'RESIDENCE_STREET_NUMBER',
        'HOUSE_SUFFIX', # Typically contains 1/2
        'PRE_DIRECTION',
        'STREET_NAME',
        'STREET_TYPE',
        'SUFFIX_DIRECTION',
        'RESIDENCE_EXTENSION', # Lot #, Apt #, etc.
        'CITY',
        'STATE',
        'ZIP',
        'MAIL_ADDR_1',
        'MAIL_ADDR_2',
        'MAIL_ADDR_3',
        'MAIL_ADDR_4',
        'MAIL_ADDR_5',
        'STATE_VOTER_REF',
        'COUNTYCODE', # 1-83
        'JURISDICTION',
        'WARD_PRECINCT',
        'SCHOOL_CODE',
        'LOWER_HOUSE_DIST',
        'UPPER_HOUSE_DIST',
        'CONGRESSIONAL_DIST',
        'COUNTY_BOARD_DIST',
        'VILLAGE_CODE',
        'VILLAGE_PRECINCT',
        'SCHOOL_PRECINCT',
        'PERMANENT_ABSENTEE_IND', # Y or N
        'REGISTRATION_STATUS', # A - active, V - verify, C - cancelled, R - rejected, CH - challenged
        'UOCAVA_STATUS' # M - military, C - Civilian overseas, N - Non UOCAVA, O - Other/Legacy Overseas
    ]

    # Odd glitch where ~40 voters' gender is shown as 1 or 2
    gender_map = {
        'F': 'F',
        'M': 'M',
        '2': 'F',
        '1': 'M',
        '': 'U',
        ' ': 'U'
    }

    # Managing required types within subclass
    col_type_dict = BaseTransformer.col_type_dict.copy()
    col_type_dict['RACE'] = set([str, type(None)])
    col_type_dict['PRECINCT_SPLIT'] = set([str, type(None)])
    col_type_dict['COUNTY_VOTER_REF'] = set([str, type(None)])


    #### Demographics methods #################################################

    def extract_gender(self, input_dict):
        return {'GENDER': self.gender_map[input_dict['GENDER']]}

    def extract_birthdate(self, input_dict):
        return {
            'BIRTHDATE': date(int(input_dict['BIRTH_YEAR']), 1, 1),
            'BIRTHDATE_IS_ESTIMATE': 'Y'
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
            'HOUSE_NUM_CHARACTER',
            'RESIDENCE_STREET_NUMBER',
            'HOUSE_SUFFIX',
            'PRE_DIRECTION',
            'STREET_NAME',
            'STREET_TYPE',
            'SUFFIX_DIRECTION',
            'RESIDENCE_EXTENSION'
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
                'STATE_NAME': input_dict['STATE'],
                'ZIP_CODE': input_dict['ZIP'],
                'VALIDATION_STATUS': '2'
            })
            converted_addr.update(raw_dict)
        else:
            converted_addr = self.constructEmptyResidentialAddress()
            converted_addr.update(raw_dict)
            converted_addr.update({
                'STATE_NAME': input_dict['STATE'],
                'VALIDATION_STATUS': '1'
            })

        return converted_addr

    def extract_mailing_address(self, input_dict):
        # columns to create address, in order
        address_components = [
            'MAIL_ADDR_1',
            'MAIL_ADDR_2',
            'MAIL_ADDR_3',
            'MAIL_ADDR_4',
            'MAIL_ADDR_5'
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
                    'MAIL_STATE': usaddress_dict.get('StateName', None),
                    'MAIL_COUNTRY': 'USA'
                }
            else:
                mail_addr_dict = {}
        return mail_addr_dict

    #### Political methods ####################################################
    def extract_state_voter_ref(self, input_dict):
        return {'STATE_VOTER_REF' : 'MI' + input_dict['STATE_VOTER_REF']}

    def extract_registration_date(self, input_dict):
        return {'REGISTRATION_DATE': self.convert_date(input_dict['DATE_OF_REGISTRATION'])}

    # TODO: Check if this would be absentee status or UOCAVA
    def extract_absentee_type(self, input_dict):
        return {'ABSENTEE_TYPE': input_dict['PERMANENT_ABSENTEE_IND']}

    # TODO: Maps back to schools in Documents/schoolcd.lst, want to find the
    # best way of bringing that in without changing this too much
    def extract_school_board_dist(self, input_dict):
        return {'SCHOOL_BOARD_DIST': input_dict['SCHOOL_CODE']}

    # HISTORY METHODS
    hist_state_voter_ref = extract_state_voter_ref

    def hist_election_info(self, input_dict):
        return {
            'ELECTION_DATE': self.convert_date(input_dict['ELECTION_DATE']),
            'ELECTION_TYPE': input_dict['ELECTION_TYPE'],
            'VOTE_METHOD': input_dict['ABSENTEE_TYPE']
        }



if __name__ == '__main__':
    preparer = StatePreparer(*sys.argv[1:])
    preparer.process()

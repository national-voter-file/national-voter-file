from oh_transformer import OHTransformer
import os

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))),
    'data')


if __name__ == '__main__':
    # Used Sandusky County data from https://www6.sos.state.oh.us/ords/f?p=111:1
    input_fields = [
        'SOS_VOTERID',
        'COUNTY_NUMBER',
        'COUNTY_ID',
        'LAST_NAME',
        'FIRST_NAME',
        'MIDDLE_NAME',
        'SUFFIX',
        'DATE_OF_BIRTH',
        'REGISTRATION_DATE',
        'VOTER_STATUS',
        'PARTY_AFFILIATION',
        'RESIDENTIAL_ADDRESS1',
        'RESIDENTIAL_SECONDARY_ADDR',
        'RESIDENTIAL_CITY',
        'RESIDENTIAL_STATE',
        'RESIDENTIAL_ZIP',
        'RESIDENTIAL_ZIP_PLUS4',
        'RESIDENTIAL_COUNTRY',
        'RESIDENTIAL_POSTALCODE',
        'MAILING_ADDRESS1',
        'MAILING_SECONDARY_ADDRESS',
        'MAILING_CITY',
        'MAILING_STATE',
        'MAILING_ZIP',
        'MAILING_ZIP_PLUS4',
        'MAILING_COUNTRY',
        'MAILING_POSTAL_CODE',
        'CAREER_CENTER',
        'CITY',
        'CITY_SCHOOL_DISTRICT',
        'COUNTY_COURT_DISTRICT',
        'CONGRESSIONAL_DISTRICT',
        'COURT_OF_APPEALS',
        'EDU_SERVICE_CENTER_DISTRICT',
        'EXEMPTED_VILL_SCHOOL_DISTRICT',
        'LIBRARY',
        'LOCAL_SCHOOL_DISTRICT',
        'MUNICIPAL_COURT_DISTRICT',
        'PRECINCT_NAME',
        'PRECINCT_CODE',
        'STATE_BOARD_OF_EDUCATION',
        'STATE_REPRESENTATIVE_DISTRICT',
        'STATE_SENATE_DISTRICT',
        'TOWNSHIP',
        'VILLAGE',
        'WARD'
    ]

    # Fieldnames listed, but can be omitted because they're the column names
    oh_transformer = OHTransformer(date_format='%Y-%m-%d', sep=',')
    oh_transformer(
        os.path.join(DATA_DIR, 'Ohio', 'SWVF_1_44_SAMPLE.csv'),
        os.path.join(DATA_DIR, 'Ohio', 'SWVF_1_44_SAMPLE_OUT.csv'),
    )

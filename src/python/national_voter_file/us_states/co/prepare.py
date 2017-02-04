from src.main.python.transformers.co_transformer import COTransformer
from src.main.python.transformers import DATA_DIR
import os


if __name__ == '__main__':
    input_fields = [
        'VOTER_ID',
        'COUNTY_CODE',
        'COUNTY',
        'LAST_NAME',
        'FIRST_NAME',
        'MIDDLE_NAME',
        'NAME_SUFFIX',
        'VOTER_NAME',
        'STATUS_CODE',
        'PRECINCT_NAME',
        'ADDRESS_LIBRARY_ID',
        'HOUSE_NUM',
        'HOUSE_SUFFIX',
        'PRE_DIR',
        'STREET_NAME',
        'STREET_TYPE',
        'POST_DIR',
        'UNIT_TYPE',
        'UNIT_NUM',
        'ADDRESS_NON_STD',
        'RESIDENTIAL_ADDRESS',
        'RESIDENTIAL_CITY',
        'RESIDENTIAL_STATE',
        'RESIDENTIAL_ZIP_CODE',
        'RESIDENTIAL_ZIP_PLUS',
        'EFFECTIVE_DATE',
        'REGISTRATION_DATE',
        'STATUS',
        'STATUS_REASON',
        'BIRTH_YEAR',
        'GENDER',
        'PRECINCT',
        'SPLIT',
        'VOTER_STATUS_ID',
        'PARTY',
        'PARTY_AFFILIATION_DATE',
        'PHONE_NUM',
        'MAIL_ADDR1',
        'MAIL_ADDR2',
        'MAIL_ADDR3',
        'MAILING_CITY',
        'MAILING_STATE',
        'MAILING_ZIP_CODE',
        'MAILING_ZIP_PLUS',
        'MAILING_COUNTRY',
        'SPL_ID',
        'PERMANENT_MAIL_IN_VOTER',
        'CONGRESSIONAL',
        'STATE_SENATE',
        'STATE_HOUSE',
        'ID_REQUIRED'
    ]

    # Fieldnames listed, but can be omitted because they're the column names
    co_transformer = COTransformer(date_format="%m/%d/%Y", sep=',')
    co_transformer(
        os.path.join(DATA_DIR, 'Colorado', 'Sample_Co_Voters_List.csv'),
        os.path.join(DATA_DIR, 'Colorado', 'Sample_Co_Voters_List_OUT.csv'),
    )

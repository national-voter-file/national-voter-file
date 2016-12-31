from src.main.python.transformers import base_transformer, wa_transformer, \
    co_transformer, ok_transformer, oh_transformer
from faker import Faker
import random
from random import randint
from string import ascii_uppercase
import csv
import os


TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')
NUM_ROWS = 100

fake = Faker()


OHIO_SCHEMA = {
    'SOS_VOTERID': lambda: 'OH{}'.format(str(randint(1000, 999999)).zfill(10)),
    'COUNTY_NUMBER': lambda: str(randint(1, 88)),
    'COUNTY_ID': lambda: str(randint(1000, 999999999)),
    'LAST_NAME': lambda: fake.last_name().upper(),
    'FIRST_NAME': lambda: fake.first_name().upper(),
    'MIDDLE_NAME': lambda: random.choice([fake.first_name().upper(), '']),
    'SUFFIX': lambda: random.choice([fake.suffix().upper(), '']),
    'DATE_OF_BIRTH': lambda: fake.date(pattern='%m/%d/%Y'),
    'REGISTRATION_DATE': lambda: fake.date(pattern='%m/%d/%Y'),
    'VOTER_STATUS': lambda: random.choice(['ACTIVE', 'INACTIVE', 'CONFIRMATION']),
    'PARTY_AFFILIATION': lambda: random.choice(list(oh_transformer.OHTransformer.ohio_party_map.keys())),
    'RESIDENTIAL_ADDRESS1': lambda: fake.street_address().upper(),
    'RESIDENTIAL_SECONDARY_ADDR': lambda: random.choice([fake.street_address().upper(), '']),
    'RESIDENTIAL_CITY': lambda: fake.city().upper(),
    'RESIDENTIAL_STATE': 'OH',
    'RESIDENTIAL_ZIP': lambda: fake.zipcode(),
    'RESIDENTIAL_ZIP_PLUS4': lambda: random.choice([str(randint(1000, 9999)), '']),
    'RESIDENTIAL_COUNTRY': 'USA',
    'RESIDENTIAL_POSTCODE': lambda: fake.zipcode(),
    'MAILING_ADDRESS1': lambda: random.choice([fake.street_address().upper(), '']),
    'MAILING_SECONDARY_ADDRESS': lambda: random.choice([fake.street_address().upper(), '']),
    'MAILING_CITY': lambda: random.choice([fake.city().upper(), '']),
    'MAILING_STATE': lambda: random.choice([fake.state_abbr(), '']),
    'MAILING_ZIP': lambda: random.choice([fake.zipcode(), '']),
    'MAILING_ZIP_PLUS4': lambda: random.choice([str(randint(1000, 9999)), '']),
    'MAILING_COUNTRY': lambda: random.choice([fake.state_abbr(), '']),
    'MAILING_POSTAL_CODE': lambda: random.choice([fake.zipcode(), '']),
    'CAREER_CENTER': lambda: fake.city().upper(),
    'CITY': lambda: fake.city().upper(),
    'CITY_SCHOOL_DISTRICT': lambda: fake.city().upper(),
    'COUNTY_COURT_DISTRICT': lambda: str(randint(1, 99)).zfill(2),
    'CONGRESSIONAL_DISTRICT': lambda: str(randint(1, 99)).zfill(2),
    'COURT_OF_APPEALS': lambda: str(randint(1, 99)).zfill(2),
    'EDU_SERVICE_CENTER_DISTRICT': lambda: str(randint(1, 99)).zfill(2),
    'EXEMPTED_VILL_SCHOOL_DISTRICT': lambda: random.choice([str(randint(1, 99)).zfill(2), '']),
    'LIBRARY': lambda: random.choice([str(randint(1, 99)).zfill(2), '']),
    'LOCAL_SCHOOL_DISTRICT': lambda: random.choice([str(randint(1, 99)).zfill(2), '']),
    'MUNICIPAL_COURT_DISTRICT': lambda: random.choice([str(randint(1, 99)).zfill(2), '']),
    'PRECINCT_NAME': lambda: fake.city().upper(),
    'PRECINCT_CODE': lambda: '{}-{}AB'.format(randint(10,80), random.choice(ascii_uppercase)),
    'STATE_BOARD_OF_EDUCATION': lambda: str(randint(1, 100)),
    'STATE_REPRESENTATIVE_DISTRICT': lambda: str(randint(1, 100)),
    'STATE_SENATE_DISTRICT': lambda: str(randint(1, 100)),
    'TOWNSHIP': lambda: random.choice([fake.city().upper(), '']),
    'VILLAGE': lambda: random.choice([fake.city().upper(), '']),
    'WARD': lambda: random.choice([fake.city().upper(), ''])
}


def make_state_data(state_name, state_schema):
    state_rows = []
    while len(state_rows) < NUM_ROWS:
        r  = {}
        for k in state_schema.keys():
            if callable(state_schema[k]):
                r[k] = state_schema[k]()
            else:
                r[k] = state_schema[k]
        state_rows.append(r)

    with open(os.path.join(TEST_DATA_DIR, '{}.csv'.format(state_name)), 'w') as f:
        w = csv.DictWriter(f, state_rows[0].keys())
        w.writeheader()
        w.writerows(state_rows)


if __name__ == '__main__':
    make_state_data('ohio', OHIO_SCHEMA)

from src.main.python.transformers import base_transformer, wa_transformer, \
    co_transformer, ok_transformer, oh_transformer, ny_transformer, \
    fl_transformer, nc_transformer, pa
import os
import csv
# Need to add test data

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')
BASE_TRANSFORMER_COLS = sorted(
    base_transformer.BaseTransformer.col_type_dict.keys()
)


# Because tests assert for existence of files, remove any _test.csv before tests
def setup():
    test_files = [f for f in os.listdir(TEST_DATA_DIR) if f.endswith('_test.csv')]
    for t in test_files:
        os.remove(os.path.join(TEST_DATA_DIR, t))


# Util for reading output file in tests
def read_transformer_output(test_filename):
    test_dict_list = []
    with open(os.path.join(TEST_DATA_DIR, test_filename)) as test_f:
        test_reader = csv.DictReader(test_f)
        for row in test_reader:
            test_dict_list.append(row)
    return test_dict_list


def test_wa_transformer():
    wa_test = wa_transformer.WATransformer(date_format="%m/%d/%Y", sep='\t')
    wa_test(
        os.path.join(TEST_DATA_DIR, 'washington.csv'),
        os.path.join(TEST_DATA_DIR, 'washington_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'washington_test.csv'))
    wa_dict_list = read_transformer_output('washington_test.csv')

    assert sorted(wa_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(wa_dict_list) > 1


def test_co_transformer():
    co_test = co_transformer.COTransformer(date_format='%m/%d/%Y', sep=',')
    co_test(
        os.path.join(TEST_DATA_DIR, 'colorado.csv'),
        os.path.join(TEST_DATA_DIR, 'colorado_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'colorado_test.csv'))
    co_dict_list = read_transformer_output('colorado_test.csv')

    assert sorted(co_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(co_dict_list) > 1


def test_ok_transformer():
    ok_test = ok_transformer.OKTransformer(date_format='%m/%d/%Y', sep=',')
    # Need to add more substantial test data
    ok_test(
        os.path.join(TEST_DATA_DIR, 'oklahoma.csv'),
        os.path.join(TEST_DATA_DIR, 'oklahoma_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'oklahoma_test.csv'))
    ok_dict_list = read_transformer_output('oklahoma_test.csv')

    assert sorted(ok_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(ok_dict_list) > 1


def test_oh_transformer():
    oh_test = oh_transformer.OHTransformer(date_format='%m/%d/%Y', sep=',')
    oh_test(
        os.path.join(TEST_DATA_DIR, 'ohio.csv'),
        os.path.join(TEST_DATA_DIR, 'ohio_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'ohio_test.csv'))
    oh_dict_list = read_transformer_output('ohio_test.csv')

    assert sorted(oh_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(oh_dict_list) > 1


def test_pa_transformer():
    pass

def test_ny_transformer():
    input_fields = [
		'LASTNAME',
		'FIRSTNAME',
		'MIDDLENAME',
		'NAMESUFFIX',
		'RADDNUMBER',
		'RHALFCODE',
		'RAPARTMENT',
		'RPREDIRECTION',
		'RSTREETNAME',
		'RPOSTDIRECTION',
		'RCITY',
		'RZIP5',
		'RZIP4',
		'MAILADD1',
		'MAILADD2',
		'MAILADD3',
		'MAILADD4',
		'DOB',
		'GENDER',
		'ENROLLMENT',
		'OTHERPARTY',
		'COUNTYCODE',
		'ED',
		'LD',
		'TOWNCITY',
		'WARD',
		'CD',
		'SD',
		'AD',
		'LASTVOTEDDATE',
		'PREVYEARVOTED',
		'PREVCOUNTY',
		'PREVADDRESS',
		'PREVNAME',
		'COUNTYVRNUMBER',
		'REGDATE',
		'VRSOURCE',
		'IDREQUIRED',
		'IDMET',
		'STATUS',
		'REASONCODE',
		'INACT_DATE',
		'PURGE_DATE',
		'SBOEID',
		'VoterHistory'
    ]
    ny_test = ny_transformer.NYTransformer(date_format='%Y%m%d',
                                           sep=',',
                                           input_fields=input_fields)
    ny_test(
        os.path.join(TEST_DATA_DIR, 'new_york.csv'),
        os.path.join(TEST_DATA_DIR, 'new_york_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'new_york_test.csv'))
    ny_dict_list = read_transformer_output('new_york_test.csv')

    assert sorted(ny_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(ny_dict_list) > 1


def test_fl_transformer():
    input_fields = [
        'County Code',
        'Voter ID',
		'Name Last',
		'Name Suffix',
		'Name First',
		'Name Middle',
		'Requested public records exemption',
		'Residence Address Line 1',
		'Residence Address Line 2',
		'Residence City (USPS)',
		'Residence State',
		'Residence Zipcode',
		'Mailing Address Line 1',
		'Mailing Address Line 2',
		'Mailing Address Line 3',
		'Mailing City',
		'Mailing State',
		'Mailing Zipcode',
		'Mailing Country',
		'Gender',
		'Race',
		'Birth Date',
		'Registration Date',
		'Party Affiliation',
		'Precinct',
		'Precinct Group',
		'Precinct Split',
		'Precinct Suffix',
		'Voter Status',
		'Congressional District',
		'House District',
		'Senate District',
		'County Commission District',
		'School Board District',
		'Daytime Area Code',
		'Daytime Phone Number',
		'Daytime Phone Extension',
		'Email address'
    ]
    fl_test = fl_transformer.FLTransformer(date_format='%m/%d/%Y',
                                           sep='\t',
                                           input_fields=input_fields)
    fl_test(
        os.path.join(TEST_DATA_DIR, 'florida.csv'),
        os.path.join(TEST_DATA_DIR, 'florida_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'florida_test.csv'))
    fl_dict_list = read_transformer_output('florida_test.csv')

    assert sorted(fl_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(fl_dict_list) > 1


def test_nc_transformer():
    nc_test = nc_transformer.NCTransformer(date_format='%m/%d/%Y', sep='\t')
    nc_test(
        os.path.join(TEST_DATA_DIR, 'north_carolina.csv'),
        os.path.join(TEST_DATA_DIR, 'north_carolina_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'north_carolina_test.csv'))
    nc_dict_list = read_transformer_output('north_carolina_test.csv')

    assert sorted(nc_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(nc_dict_list) > 1

def test_pa_transformer():
    state_test = pa.StateTransformer(date_format='%m/%d/%Y', sep='\t',
                                     input_fields=pa.StatePreparer.input_fields)
    state_test(
        os.path.join(TEST_DATA_DIR, 'pennsylvania.csv'),
        os.path.join(TEST_DATA_DIR, 'pennsylvania_test.csv'),
    )
    state_dict_list = read_transformer_output('pennsylvania_test.csv')
    assert sorted(state_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(state_dict_list) > 1

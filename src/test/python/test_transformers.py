from src.main.python.transformers import base_transformer, wa_transformer, \
    co_transformer, ok_transformer, oh_transformer, ny_transformer
import os
import csv
# Need to add test data

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')
BASE_TRANSFORMER_COLS = sorted(
    base_transformer.BaseTransformer.col_type_dict.keys()
)


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
        os.path.join(TEST_DATA_DIR, 'washington.txt'),
        os.path.join(TEST_DATA_DIR, 'washington_test.txt'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'washington_test.txt'))
    wa_dict_list = read_transformer_output('washington_test.txt')

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
        os.path.join(TEST_DATA_DIR, 'NewYork.csv'),
        os.path.join(TEST_DATA_DIR, 'NewYork_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'NewYork_test.csv'))
    ny_dict_list = read_transformer_output('NewYork_test.csv')

    assert sorted(ny_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(ny_dict_list) > 1

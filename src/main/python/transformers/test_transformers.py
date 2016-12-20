from co_transformer import COTransformer
from fl_transformer import FLTransformer
from nc_transformer import NCTransformer
from ny_transformer import NYTransformer
from oh_transformer import OHTransformer
from wa_transformer import WATransformer
from base_transformer import BaseTransformer
import os, csv
# Need to add test data

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')
BASE_TRANSFORMER_COLS = sorted(BaseTransformer.col_type_dict.keys())

# Util for reading output file in tests
def read_transformer_output(test_filename):
    test_dict_list = []
    with open(os.path.join(TEST_DATA_DIR, test_filename)) as test_f:
        test_reader = csv.DictReader(test_f)
        for row in test_reader:
            test_dict_list.append(row)
    return test_dict_list

def test_wa_transformer():
    wa_transformer = WATransformer(date_format="%m/%d/%Y", sep='\t')
    wa_transformer(
        os.path.join(TEST_DATA_DIR, 'washington.txt'),
        os.path.join(TEST_DATA_DIR, 'washington_test.txt'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'washington_test.txt'))
    wa_dict_list = read_transformer_output('washington_test.txt')

    assert sorted(wa_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(wa_dict_list) > 1

def test_co_transformer():
    co_transformer = COTransformer(date_format='%m/%d/%Y', sep=',')
    co_transformer(
        os.path.join(TEST_DATA_DIR, 'colorado.csv'),
        os.path.join(TEST_DATA_DIR, 'colorado_test.csv'),
    )
    assert os.path.exists(os.path.join(TEST_DATA_DIR, 'colorado_test.csv'))
    co_dict_list = read_transformer_output('colorado_test.csv')

    assert sorted(co_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(co_dict_list) > 1

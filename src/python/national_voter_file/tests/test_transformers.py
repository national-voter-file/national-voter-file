from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
from national_voter_file.us_states.all import load as load_states

from national_voter_file.transformers.csv_transformer import CsvOutput

import os
import csv

# Need to add test data

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')
BASE_TRANSFORMER_COLS = sorted(
    BaseTransformer.col_type_dict.keys()
)
TEST_STATES = ['co', 'fl', 'mi', 'nc', 'ny', 'oh', 'wa']

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


def run_state_transformer(state_test):
    state_path = state_test.transformer.StatePreparer.state_path
    input_path = os.path.join(TEST_DATA_DIR, '{}.csv'.format(state_path))
    output_path = os.path.join(TEST_DATA_DIR, '{}_test.csv'.format(state_path))

    state_transformer = state_test.transformer.StateTransformer()
    state_preparer = getattr(state_test.transformer,
                             'StatePreparer',
                             BasePreparer)(input_path,
                                           state_path,
                                           state_test.transformer,
                                           state_transformer)
    writer = CsvOutput(state_transformer)
    writer(state_preparer.process(), output_path)

    assert os.path.exists(os.path.join(TEST_DATA_DIR, '{}.csv'.format(state_path)))

    state_dict_list = read_transformer_output('{}_test.csv'.format(state_path))
    assert sorted(state_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(state_dict_list) > 1


def test_all_transformers():
    for state_test in load_states(TEST_STATES):
        run_state_transformer(state_test)

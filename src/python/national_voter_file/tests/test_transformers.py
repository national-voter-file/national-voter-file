import os
import csv

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
from national_voter_file.us_states.all import load as load_states

from national_voter_file.transformers.csv_transformer import CsvOutput

# Need to add test data

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')
BASE_TRANSFORMER_COLS = sorted(
    BaseTransformer.col_type_dict.keys()
)
BASE_HISTORY_COLS = sorted(
    BaseTransformer.history_type_dict.keys()
)

TEST_STATES = {'Colorado' : 'CO', 'Delaware' : 'DE', 'Florida' : 'FL',
               'Michigan' : 'MI', 'NorthCarolina' : 'NC',
               'NewJersey' : 'NJ', 'NewYork' : 'NY', 'Ohio' : 'OH',
               'Oklahoma' : 'OK', 'Utah' : 'UT', 'Vermont': 'VT',
               'Washington' : 'WA'}
TEST_HISTORY = {'Michigan': 'MI', 'Ohio': 'OH'}

# Because tests assert for existence of files, remove any _test.csv before tests
def setup():
    test_files = [f for f in os.listdir(TEST_DATA_DIR)
                  if f.endswith('_test.csv') or f.endswith('_test_hist.csv')]
    for t in test_files:
        os.remove(os.path.join(TEST_DATA_DIR, t))


# Util for reading output file in tests
def read_transformer_output(test_filename, state):
    test_dict_list = []
    with open(os.path.join(TEST_DATA_DIR, test_filename)) as test_f:
        test_reader = csv.DictReader(test_f)
        for row in test_reader:
            # Make sure first two characters are the state code
            assert row['STATE_VOTER_REF'][0:2] == TEST_STATES[state]
            # The rest of the characters should be digits
            assert row['STATE_VOTER_REF'][2:].isdigit()
            test_dict_list.append(row)
    return test_dict_list


def run_state_transformer(state_test):
    state_path = state_test.transformer.StatePreparer.state_path
    input_path = os.path.join(TEST_DATA_DIR, '{}.csv'.format(state_path))
    output_path = os.path.join(TEST_DATA_DIR, '{}_test.csv'.format(state_path))
    state = state_test.transformer.StatePreparer.state_name

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

    state_dict_list = read_transformer_output('{}_test.csv'.format(state_path), state)
    assert sorted(state_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
    assert len(state_dict_list) > 1


# TODO: Build this out more for states which may have totally separate files
def run_transformer_history(state_test):
    state_path = state_test.transformer.StatePreparer.state_path
    input_path = os.path.join(TEST_DATA_DIR, '{}.csv'.format(state_path))
    output_path = os.path.join(TEST_DATA_DIR, '{}_test_hist.csv'.format(state_path))
    state = state_test.transformer.StatePreparer.state_name

    state_transformer = state_test.transformer.StateTransformer()
    state_preparer = getattr(state_test.transformer,
                             'StatePreparer',
                             BasePreparer)(input_path,
                                           state_path,
                                           state_test.transformer,
                                           state_transformer,
                                           history=True)
    writer = CsvOutput(state_transformer)
    writer(state_preparer.process(), output_path, history=True)

    assert os.path.exists(os.path.join(TEST_DATA_DIR, '{}.csv'.format(state_path)))

    state_dict_list = read_transformer_output(
        '{}_test_hist.csv'.format(state_path), state
    )
    assert sorted(state_dict_list[0].keys()) == BASE_HISTORY_COLS
    assert len(state_dict_list) > 1


def test_all_transformers():
    for state_test in load_states([x.lower() for x in TEST_STATES.values()]):
        yield (run_state_transformer, state_test)


def test_all_transformers_history():
    for state_test in load_states([x.lower() for x in TEST_HISTORY.values()]):
        yield (run_transformer_history, state_test)

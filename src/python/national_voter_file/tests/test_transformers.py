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
TEST_STATES = ['fl', 'ny', 'oh', 'wa']


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


# def test_wa_transformer():
#
#     wa_test = load_states(['wa'])[0]
#
#     input_path = os.path.join(TEST_DATA_DIR, 'wa.csv')
#     output_path = os.path.join(TEST_DATA_DIR, 'wa_test.csv')
#
#     state_transformer = wa_test.transformer.StateTransformer()
#     state_preparer = getattr(wa_test.transformer,
#                              'StatePreparer',
#                              BasePreparer)(input_path,
#                                            'wa',
#                                            wa_test.transformer,
#                                            state_transformer)
#
#     writer = CsvOutput(state_transformer)
#     writer(state_preparer.process(), output_path)
#
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'wa_test.csv'))
#     wa_dict_list = read_transformer_output('wa_test.csv')
#
#     assert sorted(wa_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(wa_dict_list) > 1
#
# def test_fl_transformer():
#
#     fl_test = load_states(['fl'])[0]
#
#     input_path = os.path.join(TEST_DATA_DIR, 'fl.csv')
#     output_path = os.path.join(TEST_DATA_DIR, 'fl_test.csv')
#
#     state_transformer = fl_test.transformer.StateTransformer()
#     state_preparer = getattr(fl_test.transformer,
#                              'StatePreparer',
#                              BasePreparer)(input_path,
#                                            'fl',
#                                            fl_test.transformer,
#                                            state_transformer)
#
#     writer = CsvOutput(state_transformer)
#     writer(state_preparer.process(), output_path)
#
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'fl_test.csv'))
#     fl_dict_list = read_transformer_output('fl_test.csv')
#
#     assert sorted(fl_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(fl_dict_list) > 1
#
# def test_oh_transformer():
#
#     oh_test = load_states(['oh'])[0]
#
#     input_path = os.path.join(TEST_DATA_DIR, 'oh.csv')
#     output_path = os.path.join(TEST_DATA_DIR, 'oh_test.csv')
#
#     state_transformer = oh_test.transformer.StateTransformer()
#     state_preparer = getattr(oh_test.transformer,
#                              'StatePreparer',
#                              BasePreparer)(input_path,
#                                            'oh',
#                                            oh_test.transformer,
#                                            state_transformer)
#
#     writer = CsvOutput(state_transformer)
#     writer(state_preparer.process(), output_path)
#
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'oh_test.csv'))
#     oh_dict_list = read_transformer_output('oh_test.csv')
#
#     assert sorted(oh_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(oh_dict_list) > 1
#
#
# def test_ny_transformer():
#
#     ny_test = load_states(['ny'])[0]
#
#     input_path = os.path.join(TEST_DATA_DIR, 'ny.csv')
#     output_path = os.path.join(TEST_DATA_DIR, 'ny_test.csv')
#
#     state_transformer = ny_test.transformer.StateTransformer()
#     state_preparer = getattr(ny_test.transformer,
#                              'StatePreparer',
#                              BasePreparer)(input_path,
#                                            'ny',
#                                            ny_test.transformer,
#                                            state_transformer)
#
#     writer = CsvOutput(state_transformer)
#     writer(state_preparer.process(), output_path)
#
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'ny_test.csv'))
#     ny_dict_list = read_transformer_output('ny_test.csv')
#
#     assert sorted(ny_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(ny_dict_list) > 1


# def test_co_transformer():
#     co_test = co_transformer.COTransformer(date_format='%m/%d/%Y', sep=',')
#     co_test(
#         os.path.join(TEST_DATA_DIR, 'colorado.csv'),
#         os.path.join(TEST_DATA_DIR, 'colorado_test.csv'),
#     )
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'colorado_test.csv'))
#     co_dict_list = read_transformer_output('colorado_test.csv')
#
#     assert sorted(co_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(co_dict_list) > 1
#
#
# def test_ok_transformer():
#     ok_test = ok_transformer.OKTransformer(date_format='%m/%d/%Y', sep=',')
#     # Need to add more substantial test data
#     ok_test(
#         os.path.join(TEST_DATA_DIR, 'oklahoma.csv'),
#         os.path.join(TEST_DATA_DIR, 'oklahoma_test.csv'),
#     )
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'oklahoma_test.csv'))
#     ok_dict_list = read_transformer_output('oklahoma_test.csv')
#
#     assert sorted(ok_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(ok_dict_list) > 1
#
#
# def test_mi_transformer():
#     mi_prepare = mi.StatePreparer
#     mi_test = mi.StateTransformer(date_format='%m%d%Y', sep=',',
#                                   input_fields=mi_prepare.input_fields)
#     mi_test(
#         os.path.join(TEST_DATA_DIR, 'michigan.csv'),
#         os.path.join(TEST_DATA_DIR, 'michigan_test.csv'),
#     )
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'michigan_test.csv'))
#     mi_dict_list = read_transformer_output('michigan_test.csv')
#
#     assert sorted(mi_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(mi_dict_list) > 1
#
#

#
# def test_nc_transformer():
#     nc_test = nc_transformer.NCTransformer(date_format='%m/%d/%Y', sep='\t')
#     nc_test(
#         os.path.join(TEST_DATA_DIR, 'north_carolina.csv'),
#         os.path.join(TEST_DATA_DIR, 'north_carolina_test.csv'),
#     )
#     assert os.path.exists(os.path.join(TEST_DATA_DIR, 'north_carolina_test.csv'))
#     nc_dict_list = read_transformer_output('north_carolina_test.csv')
#
#     assert sorted(nc_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(nc_dict_list) > 1

# def test_pa_transformer():
#     state_test = pa.StateTransformer(date_format='%m/%d/%Y', sep='\t',
#                                      input_fields=pa.StatePreparer.input_fields)
#     state_test(
#         os.path.join(TEST_DATA_DIR, 'pennsylvania.csv'),
#         os.path.join(TEST_DATA_DIR, 'pennsylvania_test.csv'),
#     )
#     state_dict_list = read_transformer_output('pennsylvania_test.csv')
#     assert sorted(state_dict_list[0].keys()) == BASE_TRANSFORMER_COLS
#     assert len(state_dict_list) > 1

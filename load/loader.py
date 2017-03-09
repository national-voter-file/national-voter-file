import os
import argparse
import subprocess
import json


parser = argparse.ArgumentParser(description='Run data loading for NVF')

parser.add_argument(
    'task',
    type=str,
    choices=['dates', 'dimdata', 'precincts', 'transform', 'load'],
    help='Designates what action will be run by the loader'
)

parser.add_argument(
    '-s', '--state',
    type=str,
    required=False,
    help='''
    Indicates which state to perform the action for as its two letter abbreviation.
    Required for all commands other than "date".
    '''
)

parser.add_argument(
    '-c', '--configfile',
    default='load_conf.json',
    help='Config file (default: "load_conf.json")'
)

parser.add_argument(
    '--input_file',
    required=False,
    help='Input file for any argument, relative to data_path or other in conf'
)

parser.add_argument(
    '--report_date',
    required=False,
    help='If running load step, required, and date in format YYYY-MM-DD'
)

parser.add_argument(
    '--reporter_key',
    required=False,
    help='If running load command, the key for the associated reporter'
)


# Docker setup for local dev
def populate_date_dim(opts, conf):
    subprocess.call([
        os.path.join(conf['pdi_path'], 'pan.sh'),
        '-file', os.path.join(conf['nvf_path'], 'src', 'main', 'pdi', 'populateDateDimension.ktr')
     ])


def load_dimensional_data(opts, conf):
    dim_dir = os.path.join(conf['nvf_path'], 'dimensionaldata')

    subprocess.call([
        os.path.join(conf['pdi_path'], 'pan.sh'),
        '-file', os.path.join(conf['nvf_path'], 'src', 'main', 'pdi', 'LoadCensus.ktr'),
        '-param:censusFile={}'.format(os.path.join(dim_dir, 'census.csv')),
        '-param:filterState={}'.format(opts.state.upper()),
        '-param:lookupFile={}'.format(os.path.join(dim_dir, 'countyLookup.csv'))
     ])


# TODO: Need to figure out how to standardize, seems very different
# conf with params for each state on which params are required?
def load_precincts(opts, conf):
    # TODO: Need to standardize naming so that calling this way is possible
    # i.e. every precinct loading file is namespaced as pdi/SS/precincts.ktr
    subprocess.call([
        os.path.join(conf['pdi_path'], 'pan.sh'),
        # TODO: Potentially flag above for --update vs --save?
        '-file', os.path.join(conf['nvf_path'], 'src', 'main', 'pdi', opts.state.lower(), 'save_precincts.ktr'),
        '-param:reportDate={}'.format(opts.report_date),
        '-param:reportFile={}'.format(opts.input_file)
    ])


# Assuming will have access to run directly, won't have to use subprocess
def run_transformer(opts, conf):
    from national_voter_file.transformers.base import (BasePreparer,
                                                       BaseTransformer)
    from national_voter_file.us_states.all import load as load_states

    from national_voter_file.transformers.csv_transformer import CsvOutput

    state = load_states(['mi'])[0]
    state_path = state.transformer.StatePreparer.state_path
    input_path = os.path.join(conf['data_path'], opts.input_file)
    output_path = os.path.join(conf['data_path'], '{}_output.csv'.format(opts.state))
    
    state_transformer = state.transformer.StateTransformer()
    state_preparer = getattr(state.transformer,
                             'StatePreparer',
                             BasePreparer)(input_path,
                                           state_path,
                                           state.transformer,
                                           state_transformer)
    writer = CsvOutput(state_transformer)
    writer(state_preparer.process(), output_path)


def load_data(opts, conf):
    if not opts.input_file:
        opts.input_file = os.path.join(conf['data_path'], opts.state, '{}_output.csv'.format(opts.state))

    subprocess.call([
        os.path.join(conf['pdi_path'], 'kitchen.sh'),
        '-file', os.path.join(conf['nvf_path'], 'src', 'main', 'pdi', 'ProcessPreparedVoterFile.kjb'),
        '-param:reportDate={}'.format(opts.report_date),
        '-param:reportFile={}'.format(opts.input_file),
        # TODO: Should there be a default reporter for states if not specified?
        # Maybe pull from list of states with reporter keys for SOS?
        '-param:reporterKey={}'.format(opts.reporter_key)
    ])


if __name__ == '__main__':
    opts = parser.parse_args()

    with open(os.path.join(os.path.dirname(__file__), opts.configfile)) as f:
        conf = json.load(f)

    if opts.task != 'dates' and opts.state is None:
        raise Exception('--state is required if dates is the designated task')
    if opts.task == 'load' and opts.report_date is None:
        raise Exception('--report_date is required if load is the designated task')

    if opts.task == 'load':
        load_data(opts, conf)
    elif opts.task == 'transform':
        run_transformer(opts, conf)
    elif opts.task == 'precincts':
        load_precincts(opts, conf)
    elif opts.task == 'dimdata':
        load_dimensional_data(opts, conf)
    elif opts.task == 'dates':
        populate_date_dim(opts, conf)

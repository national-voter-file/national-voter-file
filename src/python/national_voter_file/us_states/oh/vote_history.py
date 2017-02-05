import os
import pandas as pd
from src.main.python.transformers import DATA_DIR


def clean_ohio_history(filepath):
    oh_df = pd.read_csv(filepath)

    # All election columns start with one of three prefixes, get those cols
    oh_cols = oh_df.columns.values.tolist()
    elec_prefixes = ['GENERAL', 'PRIMARY', 'SPECIAL']
    # Get unique elections and save to contextual data CSV
    EL_DIR = os.path.join(os.path.dirname(DATA_DIR),
                          'dimensionaldata',
                          'voter_file_elections')

    el_cols = [c for c in oh_cols if c.split('-')[0] in elec_prefixes]
    oh_elections = pd.DataFrame([e.split('-') for e in el_cols],
                                columns=['type', 'date'])
    oh_elections['state'] = 'OH'
    oh_elections.to_csv(os.path.join(EL_DIR, 'oh_elections.csv'), index=False)

    # Get voters to pivot of ID and election history
    oh_el_df = oh_df[['SOS_VOTERID'] + el_cols].copy().set_index('SOS_VOTERID')
    oh_el_stack = oh_el_df.stack().reset_index()

    oh_el_stack.columns = ['SOS_VOTERID', 'ELECTION_DESCR', 'ELECTION_DETAIL']
    oh_el_stack['ELECTION_TYPE'], oh_el_stack['ELECTION_DATE'] = zip(
        *oh_el_stack['ELECTION_DESCR'].str.split('-')
    )

    output_dir = os.path.join(os.path.dirname(filepath), 'history')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    oh_el_stack.to_csv(os.path.join(output_dir, 'oh_history.csv'), index=False)


if __name__ == '__main__':
    clean_ohio_history(os.path.join(DATA_DIR, 'Ohio', 'ALLEN.TXT'))

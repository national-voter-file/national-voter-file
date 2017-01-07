import pandas as pd
import os
from src.main.python.transformers import DATA_DIR


def clean_ohio_history(filepath):
    oh_df = pd.read_csv(filepath)

    # All election columns start with one of three prefixes, get those cols
    oh_cols = oh_df.columns.values.tolist()
    elec_prefixes = ['GENERAL', 'PRIMARY', 'SPECIAL']
    el_cols = [c for c in oh_cols if c.split('-')[0] in elec_prefixes]

    oh_el_df = oh_df[['SOS_VOTERID'] + el_cols].copy().set_index('SOS_VOTERID')
    oh_el_stack = oh_el_df.stack().reset_index()
    oh_el_stack.columns = ['SOS_VOTERID', 'ELECTION_DESCR', 'ELECTION_DETAIL']

    output_dir = os.path.join(os.path.dirname(filepath), 'history')
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    oh_el_stack.to_csv(os.path.join(output_dir, 'oh_history.csv'), index=False)


if __name__ == '__main__':
    clean_ohio_history(os.path.join(DATA_DIR, 'Ohio', 'ALLEN.TXT'))

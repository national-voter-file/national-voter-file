import pandas as pd
import os
import csv
from src.main.python.transformers import DATA_DIR


def clean_mi_history(el_path, input_path, output_path):
    # Read in elections mapping, write results to dimensionaldata
    EL_DIR = os.path.join(os.path.dirname(DATA_DIR),
                          'dimensionaldata',
                          'voter_file_elections')

    el_col_map = {}
    with open(el_path, 'r') as ef:
        for row in ef:
            el_col_map[row[:13].strip()] = {
                'DATE': row[13:21].strip(),
                'DESCR': row[21:71].strip()
            }

    with open(os.path.join(EL_DIR, 'mi_elections.csv'), 'w') as of:
        writer = csv.writer(of, delimiter=',')
        writer.writerow(['type', 'date', 'state'])
        for k in el_col_map.keys():
            writer.writerow([el_col_map[k]['DESCR'], el_col_map[k]['DATE'], 'MI'])

    # Create history dir if doesn't already exist
    if not os.path.exists(os.path.dirname(output_path)):
        os.mkdir(os.path.dirname(output_path))

    # Use mapping to write voter history consolidated file
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        writer = csv.writer(outfile, delimiter=',')
        writer.writerow(['VOTER_ID', 'DATE', 'DESCR', 'STATE'])
        for row in infile:
            el_key = row[25:38].strip()
            out_row = [row[:13].strip(),
                       el_col_map[el_key]['DATE'],
                       el_col_map[el_key]['DESCR'],
                       'MI']
            writer.writerow(out_row)


if __name__ == '__main__':
    MI_DIR = os.path.join(DATA_DIR, 'Michigan')
    EL_PATH = os.path.join(MI_DIR, 'electionscd.lst')
    HIST_PATH = os.path.join(MI_DIR, 'entire_state_h.lst')
    OUT_PATH = os.path.join(MI_DIR, 'history', 'mi_history.csv')

    clean_mi_history(EL_PATH, HIST_PATH, OUT_PATH)

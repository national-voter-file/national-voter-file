import requests
import sys
import csv
import re
import os
from src.main.python.transformers import DATA_DIR


GITHUB_API = 'https://api.github.com/'
REPO_RE = re.compile(r'openelections-(data|results)-\w{2}')
DATA_RE = re.compile(r'\d{8}__\w{2}__\w{5,10}.*\.csv')


def process_repo_contents(repo, repo_json, match_path, un, pw):
    el_files = []
    rel_paths = [r['name'] for r in repo_json if isinstance(r, dict) and re.match(match_path, r['name'])]
    for p in rel_paths:
        path_json = requests.get('{}repos/openelections/{}/contents/{}?per_page=100'.format(GITHUB_API, repo, p),
                                 auth=(un, pw)).json()
        path_files = [f['name'] for f in path_json if isinstance(f, dict) and DATA_RE.match(f['name'])]
        el_files = el_files + path_files
    return el_files


def repo_election_files(repos, un, pw):
    el_files = []
    data_repos = [r for r in repos if '-data-' in r]
    result_repos = [r for r in repos if '-results-' in r]

    for r in repos:
        resp = requests.get('{}repos/openelections/{}/contents?per_page=100'.format(GITHUB_API, r),
                            auth=(un, pw))
        if '-data-' in r:
            el_files = el_files + process_repo_contents(r, resp.json(), r'\d{4}', un, pw)
        elif '-results-' in r:
            el_files = el_files + process_repo_contents(r, resp.json(), r'raw', un, pw)

    return el_files


if __name__ == '__main__':
    # Accepts 2 command line arguments, GitHub username and password for basic
    # auth, otherwise will hit the limit on unauthenticated requests
    DIM_DATA_DIR = os.path.join(os.path.dirname(DATA_DIR), 'dimensionaldata')
    oe_resp = requests.get(GITHUB_API + 'orgs/openelections/repos?per_page=100',
                           auth=(sys.argv[1], sys.argv[2]))

    oe_repos = [r['name'] for r in oe_resp.json() if REPO_RE.match(r['name'])]

    el_files = repo_election_files(oe_repos, sys.argv[1], sys.argv[2])
    with open(os.path.join(DIM_DATA_DIR, 'openelections_data.csv'), 'w') as f:
        oe_writer = csv.writer(f, delimiter=',')
        oe_writer.writerow(['openelections_file_name', 'date', 'state', 'type'])
        for row in el_files:
            spl_row = row.split('__')
            oe_writer.writerow([row, spl_row[0], spl_row[1], spl_row[2]])

    print(len(el_files))

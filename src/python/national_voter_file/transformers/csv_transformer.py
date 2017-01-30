import csv
import os
import zipfile
import argparse 

from national_voter_file.transformers.base import (DATA_DIR,
                                                   BasePreparer,
                                                   BaseTransformer)
from national_voter_file.us_states.all import load as load_states

parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument("-s", "--states", dest="states", metavar="US_STATES",
                    required=True,
                    help="comma-separated list of state (postal initials) to parse")
parser.add_argument("-d", "--datadir",
                    dest="input_path", default=DATA_DIR, metavar="INPUT_PATH",
                    help="input path or directory (default is ./data/ from where you run the command)")
parser.add_argument("-o", "--outputdir",
                    dest="output_path", default=DATA_DIR, metavar="OUTPUT_PATH",
                    help="output path or directory (default is ./data/ from where you run the command)")

class CsvOutput(object):

    def __init__(self, state_transformer):
        self.state_transformer = state_transformer

    def __call__(self, input_iter, output_path):
        """
        Set paths here
        Fails if any methods aren't implemented
        Should not be overwritten in the subclass, this method enforces a
        similar check on all data created
        """
        with self.open(output_path, 'w') as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames = sorted(BaseTransformer.col_type_dict.keys()),
            )
            writer.writeheader()
            for input_dict in input_iter:
                output_dict = self.state_transformer.process_row(input_dict)
                output_dict = self.state_transformer.fix_missing_mailing_addr(output_dict)

                self.state_transformer.validate_output_row(output_dict) # validate here
                writer.writerow(output_dict)

    open = BasePreparer.open


def main():
    args = parser.parse_args()
    states = args.states.split(',')
    state_mods = load_states(states)
    for i,s in enumerate(state_mods):
        state = states[i]
        input_path = args.input_path
        output_path = args.output_path

        state_transformer = s.transformer.StateTransformer()
        state_preparer = getattr(s.transformer,
                                 'StatePreparer',
                                 BasePreparer)(input_path,
                                               state,
                                               s.transformer,
                                               state_transformer)

        if os.path.isdir(output_path):
            output_path = os.path.join(output_path, '%s_output.csv' % state)

        writer = CsvOutput(state_transformer)
        writer(state_preparer.process(), output_path)


if __name__ == "__main__":
    main()

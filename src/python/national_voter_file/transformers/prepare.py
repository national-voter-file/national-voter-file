from src.main.python.transformers.wa_transformer import WATransformer
from os.path import dirname
import os

DATA_DIR = os.path.join(
    dirname(dirname(dirname(dirname(dirname(__file__))))), 'testdata'
)

if __name__ == '__main__':
    wa_transformer = WATransformer(date_format="%m/%d/%Y", sep='\t')
    wa_transformer(
        os.path.join(DATA_DIR, 'Washington', 'Washington State Data.txt'),
        os.path.join(DATA_DIR, 'output_test.csv'),
    )

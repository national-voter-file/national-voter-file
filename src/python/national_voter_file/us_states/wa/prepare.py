from src.main.python.transformers.wa_transformer import WATransformer
from src.main.python.transformers import DATA_DIR
import os


if __name__ == '__main__':
    wa_transformer = WATransformer(date_format="%m/%d/%Y", sep='\t')
    wa_transformer(
        os.path.join(DATA_DIR, 'Washington', '201605_VRDB_ExtractSAMPLE.txt'),
        os.path.join(DATA_DIR, 'Washington', '201605_VRDB_ExtractSAMPLE_OUT.csv')
    )

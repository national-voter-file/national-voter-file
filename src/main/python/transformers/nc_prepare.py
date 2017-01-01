from src.main.python.transformers.nc_transformer import NCTransformer
from src.main.python.transformers import DATA_DIR
import os


if __name__ == '__main__':
    nc_transformer = NCTransformer(date_format="%m/%d/%Y", sep='\t')
    nc_transformer(
        os.path.join(DATA_DIR, 'NorthCarolina', 'ncvoter_StatewideSAMPLE.csv'),
        os.path.join(DATA_DIR, 'NorthCarolina', 'ncvoter_StatewideSAMPLE_OUT.csv'),
    )

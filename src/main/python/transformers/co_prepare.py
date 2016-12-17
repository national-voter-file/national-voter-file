from co_transformer import COTransformer
import os

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd())))),
    'data')

if __name__ == '__main__':
    co_transformer = COTransformer(date_format="%m/%d/%Y", sep=',')
    co_transformer(
        os.path.join(DATA_DIR, 'Sample_Co_Voters_List.csv'),
        os.path.join(DATA_DIR, 'Sample_Co_Voters_List_OUT.csv'),
    )

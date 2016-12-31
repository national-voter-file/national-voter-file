from src.main.python.transformers.nc_transformer import NCTransformer

if __name__ == '__main__':
    nc_transformer = NCTransformer(date_format="%m/%d/%Y", sep='\t')
    nc_transformer(
        '../../../../data/NorthCarolina/ncvoter_StatewideSAMPLE.csv',
        '../../../../data/NorthCarolina/ncvoter_StatewideSAMPLE_OUT.csv',
    )

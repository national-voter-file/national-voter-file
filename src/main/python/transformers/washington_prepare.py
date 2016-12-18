from wa_transformer import WATransformer

if __name__ == '__main__':
    wa_transformer = WATransformer(date_format="%m/%d/%Y", sep='\t')
    wa_transformer(
        '../../../../data/Washington/201605_VRDB_ExtractSAMPLE.txt',
        '../../../../data/Washington/201605_VRDB_ExtractSAMPLE_OUT.csv',
    )

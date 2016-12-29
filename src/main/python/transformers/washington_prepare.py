from wa_transformer import WATransformer

if __name__ == '__main__':
    wa_transformer = WATransformer(date_format="%m/%d/%Y", sep='\t')
    wa_transformer(
        '/national-voter-file/data/Washington/201605_VRDB_ExtractSAMPLE.txt',
        '/national-voter-file/data/Washington/201605_VRDB_ExtractSAMPLE_OUT.csv',
    )

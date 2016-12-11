from wa_transformer import WATransformer

if __name__ == '__main__':
    wa_transformer = WATransformer(date_format="%m/%d/%Y", sep='\t')
    wa_transformer(
        '../../../../testdata/Washington/Washington State Data.txt',
        '../../../../testdata/output_test.csv',
    )

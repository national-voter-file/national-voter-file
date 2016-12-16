from oh_transformer import OHTransformer

if __name__ == '__main__':
    oh_transformer = OHTransformer(date_format="%m/%d/%Y", sep=',')
    # Used Sandusky County data from https://www6.sos.state.oh.us/ords/f?p=111:1
    oh_transformer(
        '../../../../data/Ohio/SWVF_1_44_SAMPLE.csv',
        '../../../../data/Ohio/SWVF_1_44_SAMPLE_OUT.csv',
    )

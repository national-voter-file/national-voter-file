from oh_transformer import OHTransformer

if __name__ == '__main__':
    oh_transformer = OHTransformer(date_format="%Y-%m-%d", sep=',')
    # Used Sandusky County data from https://www6.sos.state.oh.us/ords/f?p=111:1
    oh_transformer(
        # '../../../../../SANDUSKY.TXT',
        # '../../../../../sandusky_test_out.csv',
    )

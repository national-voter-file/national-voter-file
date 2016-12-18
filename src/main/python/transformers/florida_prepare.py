from fl_transformer import FLTransformer

if __name__ == '__main__':
    input_fields = [
				'County Code',
        		'Voter ID',
        		'Name Last',
        		'Name Suffix',
        		'Name First',
        		'Name Middle',
        		'Requested public records exemption',
        		'Residence Address Line 1',
        		'Residence Address Line 2',
        		'Residence City (USPS)',
        		'Residence State',
        		'Residence Zipcode',
        		'Mailing Address Line 1',
        		'Mailing Address Line 2',
        		'Mailing Address Line 3',
        		'Mailing City',
        		'Mailing State',
        		'Mailing Zipcode',
        		'Mailing Country',
        		'Gender',
        		'Race',
        		'Birth Date',
        		'Registration Date',
        		'Party Affiliation',
        		'Precinct',
        		'Precinct Group',
        		'Precinct Split',
        		'Precinct Suffix',
        		'Voter Status',
        		'Congressional District',
        		'House District',
        		'Senate District',
        		'County Commission District',
        		'School Board District',
        		'Daytime Area Code',
        		'Daytime Phone Number',
        		'Daytime Phone Extension',
        		'Email address']

    fl_transformer = FLTransformer(date_format="%m/%d/%Y", sep='\t', input_fields=input_fields)
    fl_transformer(
        '../../../../data/Florida/AllFLSample20160908.txt',
        '../../../../data/Florida/AllFLSample20160908_OUT.csv',
    )

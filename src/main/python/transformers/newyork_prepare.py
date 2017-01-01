from src.main.python.transformers.ny_transformer import NYTransformer
from src.main.python.transformers import DATA_DIR
import os


if __name__ == '__main__':
    input_fields = [
		'LASTNAME',
		'FIRSTNAME',
		'MIDDLENAME',
		'NAMESUFFIX',
		'RADDNUMBER',
		'RHALFCODE',
		'RAPARTMENT',
		'RPREDIRECTION',
		'RSTREETNAME',
		'RPOSTDIRECTION',
		'RCITY',
		'RZIP5',
		'RZIP4',
		'MAILADD1',
		'MAILADD2',
		'MAILADD3',
		'MAILADD4',
		'DOB',
		'GENDER',
		'ENROLLMENT',
		'OTHERPARTY',
		'COUNTYCODE',
		'ED',
		'LD',
		'TOWNCITY',
		'WARD',
		'CD',
		'SD',
		'AD',
		'LASTVOTEDDATE',
		'PREVYEARVOTED',
		'PREVCOUNTY',
		'PREVADDRESS',
		'PREVNAME',
		'COUNTYVRNUMBER',
		'REGDATE',
		'VRSOURCE',
		'IDREQUIRED',
		'IDMET',
		'STATUS',
		'REASONCODE',
		'INACT_DATE',
		'PURGE_DATE',
		'SBOEID',
		'VoterHistory']

    print("Data Dir " + DATA_DIR)

    ny_transformer = NYTransformer(date_format="%Y%m%d",
                                   sep=',',
                                   input_fields=input_fields)
    ny_transformer(
        os.path.join(DATA_DIR, 'NewYork', 'AllNYSVoters20160831SAMPLE.txt'),
        os.path.join(DATA_DIR, 'NewYork', 'AllNYSVoters20160831SAMPLE_OUT.csv')
    )

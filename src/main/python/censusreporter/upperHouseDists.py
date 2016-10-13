import pandas as pd
from censusreporter_api import *

cd = pd.read_csv("C:/dev/GitHub/getmovement/national-voter-file/data/NewYork/StateSenate.txt")
cd['FIPS']=cd.apply(lambda row:"62000US%5d"%(int(row['GEOID'])),axis=1)
c = cd.set_index('FIPS')

geoids = cd['FIPS'].tolist()
df = get_dataframe(geoids=geoids, tables=['B01001','B01003', 'B03002','B06008','B23001', 'B19001','B25009','B25077'])

data = pd.concat([c, df],axis=1, join_axes=[c.index])
data.to_csv("C:/dev/GitHub/getmovement/national-voter-file/data/NewYork/StateSenate_OUT.txt")
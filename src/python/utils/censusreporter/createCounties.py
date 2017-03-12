import pandas as pd
from censusreporter_api import *

counties = pd.read_csv("C:/dev/GitHub/getmovement/national-voter-file/data/NewYork/counties.csv")
counties['FIPS']=counties.apply(lambda row:"05000US%02d%03d"%(int(row['STATEFP']), int(row['COUNTYFP'])),axis=1)
c = counties.set_index('FIPS')

geoids = counties['FIPS'].tolist()
df = get_dataframe(geoids=geoids, tables=['B01001','B01003', 'B03002','B06008','B23001', 'B19001','B25009','B25077'])

data = pd.concat([c, df],axis=1, join_axes=[c.index])
data.to_csv("C:/dev/GitHub/getmovement/national-voter-file/data/NewYork/counties_OUT.csv")
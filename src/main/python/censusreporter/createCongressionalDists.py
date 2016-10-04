import pandas as pd
from censusreporter_api import *

cd = pd.read_csv("C:/dev/GitHub/getmovement/national-voter-file/data/NewYork/congressioal_dists.csv")
cd['FIPS']=cd.apply(lambda row:"50000US%02d%02d"%(int(row['D113']), int(row['FP'])),axis=1)
c = cd.set_index('FIPS')

geoids = cd['FIPS'].tolist()
df = get_dataframe(geoids=geoids, tables=['B01001','B01003', 'B03002','B06008','B23001', 'B19001','B25009','B25077'])

data = pd.concat([c, df],axis=1, join_axes=[c.index])
data.to_csv("C:/dev/GitHub/getmovement/national-voter-file/data/NewYork/congressioal_dists_OUT.csv")
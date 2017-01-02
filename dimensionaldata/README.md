# Dimensional Data
This directory is used by the docker scripts to find data that will be loaded into some of the dimension tables.
There is no actual data stored in the git repo since they are rather large. You can download a copy of these files from our
dropbox (ask on the slack channel for access to a share).

If you wish, you can generate your own copy of these files with the python script src/main/python/censusreporter/getCensus.python

# countyLookup.csv
This file contains the lookups between the state voter file codes for counties and their
official census FIPS codes. The best place to get the FIPS codes to add to this file is
from the [2010 FIPS Codes for Counties and County Equivalent Entities](https://www.census.gov/geo/reference/codes/cou.html)

There is a .gitignore on this directory which insures these big files are never committed to the repo.

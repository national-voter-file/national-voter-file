# Relational Diagrams

The relational diagrams in this directory were built using
the free online data modeling tool [ERDplus][erdplus]. The
_*.erdplus_ files are text files containing one large JSON object.

To view the files:

1. Sign up for a free account on [ERDplus][erdplus].
2. Navigate to [Diagrams][diagrams].
3. Select the *Import* button and import the _*.erdplus_ file.

You can then edit, save, and export it...and convert it to
starting SQL data definitions (although we have to add some
PostgreSQL-specific things).


Specifications for some columns:

| Table | Column | type | description |
| ----- | ------ | ----:| ----------- |
| `public.census_dim` | `entity_fips` | varchar(5) | The FIPS code. |
| `public.census_dim` | `entity_type` | varchar(10) | The type identifier to make the FIPS code unique (1) |
| `public.census_dim` | `entity_name` | varchar(64) | The actual place name (e.g. 'alabama') |


(1) The categories of FIPS identifier are
'STATEFP', 'CD', 'SLDU', 'SLDL', 'VTD', 'SDELM', 'SDSEC', 'SDUNI',
'INCPLACEFP', 'CDPFP', 'AIANNHCE', or 'COUNTYFP' ([described here][nlt]).


[erdplus]: https://erdplus.com
[diagrams]: https://erdplus.com/#/diagrams
[nlt]: https://www.census.gov/geo/maps-data/data/nlt_description.html

create table "ny-ed"
(
	gid serial not null
		constraint "ny-ed_pkey2"
			primary key,
	county_fips varchar(5),
	county_name VARCHAR(50),
	ad  INTEGER,
	ed INTEGER,
	precinct_ed varchar(254),
	county_ed varchar(50),
	geom geometry(MultiPolygon,4326)
)
;

create index "ny-ed_geom_idx2"
	on "nys-ed" USING GIST(geom)
;
 alter TABLE "ny-ed" alter COLUMN ad TYPE CHAR(3);

truncate table "ny-ed";
-- Populate shapes for EDs ex city
insert into "ny-ed" (county_name, precinct_ed, county_ed, geom)
	select county, precinctna, countyed, geom from "nys-ed"
	where county not in ('NEW YORK', 'BRONX', 'KINGS', 'QUEENS', 'RICHMOND');

-- lookup fips code for county
update "ny-ed" ed set county_fips = (select fips from county_dim c where ed.county_name || ' COUNTY' = upper(c.county_name));

-- Add city EDs
insert into "ny-ed" (county_name, county_fips, precinct_ed, ad, ed, geom)
	select upper(name), '36'||county, to_char(elect_dist,'00-000'), substring(to_char(elect_dist,'00-000'), '(\d\d).*'), substring(to_char(elect_dist,'00-000'), '.*-(\d\d\d)'),  "nyc-ed".geom
	from "nyc-ed" join cty036 on st_contains(cty036.geom, "nyc-ed".geom)

select DISTINCT county from "nys-ed"  order by county limit 100;
select * from "ny-ed" ed join county_dim c on ed.county_name || ' COUNTY' = upper(c.county_name) limit 100;

select * from "ny-ed" ed join county_dim c on ed.county_fips = c.fips;
select upper(county_name) from county_dim where state_name = 'NY'
update "ny-ed" set ed =  substring(precinct_ed, '.* ED (\d\d\d)');

select county_name from "ny-ed" limit 100;
select * from "nyc-ed" limit 20;

update "ny-ed" set;
select to_char(elect_dist,'00-000'), substring(to_char(elect_dist,'00-000'), '(\d\d).*'), substring(to_char(elect_dist,'00-000'), '.*-(\d\d\d)')  from "nyc-ed" limit 100;
select * from "ny-ed" where "ny-ed".county_name is null limit 100;

select * from county_dim where county_name like 'Saint%'

ALTER TABLE cb_2016_36_sldl_500k
    ALTER COLUMN geom TYPE geometry(MultiPolygon,4326) USING ST_Transform(geom,4326);


SELECT srid, srtext, proj4text FROM spatial_ref_sys WHERE srtext ILIKE '%D_North_American_1983%'

select upper(name), '36'||county from "nyc-ed" join cty036 on st_contains(cty036.geom, "nyc-ed".geom) limit 100;

select * from county_dim where state_name = 'NY';
select * from "ny-ed" limit 100;


select * from census_blocks limit 100;
select * from "ny-ed";
select geoid10 precinct_ed, geoid10,  st_intersects(ed.geom, c.geom),  ST_Intersection(ed.geom, c.geom), ST_Area(ST_Intersection(ed.geom, c.geom)) * 10000 as intersect_area
	 from "ny-ed" ed
	join census_blocks c on county_fips = countyfp10
where county_name = 'ALBANY' and st_intersects(ed.geom, c.geom) limit 10;
update "ny-ed" set county_fips = substring(county_fips, '..(.*)');


select geoid10 precinct_ed, geoid10,  st_intersects(ed.geom, c.geom),  ST_Intersection(ed.geom, c.geom) as geom, ST_Area(ST_Intersection(ed.geom, c.geom)) * 10000 as intersect_area
into  TABLE  census_block_test
	 from "ny-ed" ed
	join census_blocks c on county_fips = countyfp10
where county_name = 'ALBANY' and st_intersects(ed.geom, c.geom) ;

select * from census_block_ed_map;

create view census_block_ed as select c1.gid, c2.geoid10, c1.precinct_ed, c2.geom, intersect_area from census_block_ed_map c1 join census_blocks c2 on c1.gid = c2.gid
drop view census_block_ed;
drop table census_block_ed_map;

select * from census_block_ed_map where geoid10 = '360010145023005'

select distinct on(geoid10) precinct_ed, geoid10, st_area(st_intersection(c.geom, ed.geom)) as intersect_area
	from census_blocks c, "ny-ed" ed
	where geoid10='360010145011009' and ed.precinct_ed in ('NEW SCOTLAND - ED 005', 'NEW SCOTLAND - ED 006', 'NEW SCOTLAND - ED 007')
order by geoid10, intersect_area desc, precinct_ed desc;

select  precinct_ed, geoid10, st_area(st_intersection(c.geom, ed.geom)) as intersect_area,st_intersects(c.geom, ed.geom)
	from census_blocks c, "ny-ed" ed
	where geoid10='360010146111027' and ed.precinct_ed in ('NEW SCOTLAND - ED 005', 'NEW SCOTLAND - ED 006', 'NEW SCOTLAND - ED 007', 'GUILDERLAND - ED 013')
order by intersect_area desc, geoid10 desc;


---- Census blocks
select DISTINCT ON (c.gid) c.gid, county_name, ad, ed, precinct_ed,
	c.statefp10, c.countyfp10, c.tractce10, c.blockce10, geoid10,   ST_Area(ST_Intersection(ed.geom, c.geom))/st_area(c.geom) as intersect_area
into census_block_test
 from "ny-ed" ed
	join census_blocks c on county_fips = countyfp10
where  st_intersects(ed.geom, c.geom)
ORDER BY c.gid, intersect_area desc, county_name, ad, ed, precinct_ed,
	c.statefp10, c.countyfp10, c.tractce10, c.blockce10, geoid10;


-- Assembly Dist
update "ny-ed" set ad = (select  sldlst from cb_2016_36_sldl_500k ad  where st_contains(ad.geom, "ny-ed".geom));


select distinct on (precinct_ed) precinct_ed, sldlst, ST_Area(ST_Intersection(ed.geom, ad.geom))/st_area(ad.geom) as intersect_area
into TEMPORARY table temp_ad_lookup
	from "ny-ed" ed, cb_2016_36_sldl_500k ad
	where ad is null and  st_intersects(ad.geom, ed.geom)
ORDER BY precinct_ed, intersect_area desc, sldlst;

drop table temp_ad_lookup;
select * from temp_ad_lookup join "ny-ed" on "ny-ed".precinct_ed = temp_ad_lookup.precinct_ed;
update "ny-ed" set ad = (select sldlst from temp_ad_lookup where temp_ad_lookup.precinct_ed = "ny-ed".precinct_ed);
select county_name, ad, ed from "ny-ed" where ad is null;
select * from "ny-ed" ;

select count(*) from temp_ad_lookup;
select * from temp_ad_lookup where precinct_ed = ' 23-003';

select distinct on (precinct_ed) precinct_ed, sldlst,  ST_Area(ST_Intersection(ed.geom, ad.geom))/st_area(ad.geom) as intersect_area
from "ny-ed" ed join cb_2016_36_sldl_500k ad on st_intersects(ad.geom, ed.geom) where ed.precinct_ed=' 23-003' order by precinct_ed, intersect_area desc, sldlst;

update "ny-ed"  set ad=sldlst from temp_ad_lookup where "ny-ed".ad is null and "ny-ed".precinct_ed = temp_ad_lookup.precinct_ed;

update "ny-ed" set precinct_ed =  county_name || '-'|| ad||'-'|| ed;

select * from census_block_ed_map;
select * from "ny-ed" where precinct_ed is null;

select * from "ny-ed" where county_name in ('NEW YORK', 'BRONX', 'KINGS', 'QUEENS', 'RICHMOND');


update "ny-ed" set county_ed =  county_name || 'AD '||ad||' - ED '||ed  where county_name in ('NEW YORK', 'BRONX', 'KINGS', 'QUEENS', 'RICHMOND');

select * from census_block_ed_map c join "ny-ed" e on c.precinct_ed = e.precinct_ed;
update census_block_ed_map set county_ed = "ny-ed".county_ed from "ny-ed" where "ny-ed".precinct_ed = census_block_ed_map.precinct_ed;

SELECT *
FROM census_block_ed_map where county_name in ('NEW YORK', 'BRONX', 'KINGS', 'QUEENS', 'RICHMOND');;';
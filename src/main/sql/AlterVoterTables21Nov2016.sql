--County Changes

ALTER TABLE public.county_dim ALTER COLUMN county_name TYPE character varying(75);

--Congress Changes

ALTER TABLE public.congressional_dist_dim ALTER COLUMN district_name TYPE character varying(64);

--State House Changes

ALTER TABLE public.lower_house_dist_dim ALTER COLUMN district_number TYPE character varying(3);
ALTER TABLE public.lower_house_dist_dim ALTER COLUMN district_name TYPE character varying(64);

--State Senate Changes

ALTER TABLE public.upper_house_dist_dim ALTER COLUMN district_number TYPE character varying(3);
ALTER TABLE public.upper_house_dist_dim ALTER COLUMN district_name TYPE character varying(64);





SELECT 'ALTER TABLE '||nspname||'.'||relname||' DROP CONSTRAINT '||conname||';'
FROM pg_constraint 
INNER JOIN pg_class ON conrelid=pg_class.oid 
INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace 
ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END,contype,nspname,relname,conname;


ALTER TABLE public.voter_report_fact DROP CONSTRAINT voter_report_fact_voter_key_fkey;
ALTER TABLE public.vote_fact DROP CONSTRAINT vote_fact_voter_key_fkey;
ALTER TABLE public.voter_report_fact DROP CONSTRAINT voter_report_fact_precinct_key_fkey;
ALTER TABLE public.vote_fact DROP CONSTRAINT vote_fact_precinct_key_fkey;
ALTER TABLE public.voter_report_fact DROP CONSTRAINT voter_report_fact_household_key_fkey;
ALTER TABLE public.vote_fact DROP CONSTRAINT vote_fact_household_key_fkey;

SELECT 'ALTER TABLE '||nspname||'.'||relname||' ADD CONSTRAINT '||conname||' '|| pg_get_constraintdef(pg_constraint.oid)||';'
FROM pg_constraint
INNER JOIN pg_class ON conrelid=pg_class.oid
INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END DESC,contype DESC,nspname DESC,relname DESC,conname DESC;

ALTER TABLE public.voter_report_fact ADD CONSTRAINT voter_report_fact_voter_key_fkey FOREIGN KEY (voter_key) REFERENCES voter_dim(voter_id);
ALTER TABLE public.voter_dim ADD CONSTRAINT voter_dim_pkey PRIMARY KEY (voter_id);
ALTER TABLE public.voter_report_fact ADD CONSTRAINT voter_report_fact_precinct_key_fkey FOREIGN KEY (precinct_key) REFERENCES precinct_dim(precinct_id);
ALTER TABLE public.vote_fact ADD CONSTRAINT vote_fact_precinct_key_fkey FOREIGN KEY (precinct_key) REFERENCES precinct_dim(precinct_id);
ALTER TABLE public.voter_report_fact ADD CONSTRAINT voter_report_fact_household_key_fkey FOREIGN KEY (household_key) REFERENCES household_dim(household_id);
ALTER TABLE public.vote_fact ADD CONSTRAINT vote_fact_household_key_fkey FOREIGN KEY (household_key) REFERENCES household_dim(household_id);
select * from voter_dim where voter_id in (select voter_key from voter_report_fact where reporter_key = 4 limit 100);

delete  from voter_dim where voter_id in (select voter_key from voter_report_fact where reporter_key = 4 limit 500000);


delete from household_dim where household_id in (select household_id from household_dim where state_name='MI');



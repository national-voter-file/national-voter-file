SELECT 'ALTER TABLE '||nspname||'.'||relname||' DROP CONSTRAINT '||conname||';'
FROM pg_constraint 
INNER JOIN pg_class ON conrelid=pg_class.oid 
INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace 
ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END,contype,nspname,relname,conname;


ALTER TABLE public.voter_report_fact DROP CONSTRAINT voter_report_fact_voter_key_fkey;



SELECT 'ALTER TABLE '||nspname||'.'||relname||' ADD CONSTRAINT '||conname||' '|| pg_get_constraintdef(pg_constraint.oid)||';'
FROM pg_constraint
INNER JOIN pg_class ON conrelid=pg_class.oid
INNER JOIN pg_namespace ON pg_namespace.oid=pg_class.relnamespace
ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END DESC,contype DESC,nspname DESC,relname DESC,conname DESC;

ALTER TABLE public.voter_report_fact ADD CONSTRAINT voter_report_fact_voter_key_fkey FOREIGN KEY (voter_key) REFERENCES voter_dim(voter_id);

select * from voter_dim where voter_id in (select voter_key from voter_report_fact where reporter_key = 4 limit 100);

delete  from voter_dim where voter_id in (select voter_key from voter_report_fact where reporter_key = 4 limit 500000);
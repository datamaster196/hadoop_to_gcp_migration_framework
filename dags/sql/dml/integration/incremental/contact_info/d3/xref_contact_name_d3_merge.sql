create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_name_stage` as
with combined_cust_name as
(
select contact_adw_key,
nm_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_stage`
union distinct
select
target.contact_adw_key,
target.nm_adw_key,
target.effective_start_datetime as last_upd_dt
from `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name` target
where contact_source_system_nm='d3'
 and nm_typ_cd like 'common%'
),
cust_name as
(select
contact_adw_key,
nm_adw_key,
cast(last_upd_dt as datetime) as effective_start_datetime,
lag(nm_adw_key) over (partition by contact_adw_key order by cast(last_upd_dt as datetime), nm_adw_key) as prev_name,
coalesce(lead(cast(last_upd_dt as datetime)) over (partition by contact_adw_key order by cast(last_upd_dt as datetime), nm_adw_key), datetime('9999-12-31'
) )as next_record_date
from combined_cust_name
),
set_grouping_column as
(select
contact_adw_key,
nm_adw_key,
case when prev_name is null or prev_name<>nm_adw_key then 1 else 0 end as new_name_tag,
effective_start_datetime,
next_record_date
from cust_name
), set_groups as
(select
contact_adw_key,
nm_adw_key,
sum(new_name_tag) over (partition by contact_adw_key order by effective_start_datetime, nm_adw_key) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
contact_adw_key,
nm_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
contact_adw_key,
nm_adw_key,
grouping_column
),
update_key_typ_nm as
(
select
contact_adw_key,
nm_adw_key,
effective_start_datetime,
case when effective_end_datetime=datetime('9999-12-31') then effective_end_datetime else datetime_sub(effective_end_datetime, interval 1 second) end as effective_end_datetime,
row_number() over(partition by contact_adw_key,  effective_start_datetime order by effective_end_datetime desc,nm_adw_key ) as rn
from deduped
)
select
contact_adw_key,
nm_adw_key,
'd3' as source_system,
concat('common', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as name_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
{{ dag_run.id }} as  batch_insert,
current_datetime update_datetime,
{{ dag_run.id }} as  batch_update
from update_key_typ_nm
;
merge into `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name` target
using `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_name_stage` source
on target.contact_adw_key=source.contact_adw_key and 
   target.contact_source_system_nm=source.source_system and
   target.nm_typ_cd=source.name_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and (target.effective_end_datetime!=source.effective_end_datetime or target.nm_adw_key!=source.nm_adw_key) then update
set 
target.nm_adw_key=source.nm_adw_key,
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update,
target.actv_ind=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END
when not matched by target then insert
(
contact_adw_key,    
nm_adw_key,    
contact_source_system_nm,
nm_typ_cd,    
effective_start_datetime,    
effective_end_datetime,
actv_ind,    
integrate_insert_datetime,    
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
)
values
(
source.contact_adw_key,
source.nm_adw_key,
source.source_system,
source.name_type,
source.effective_start_datetime,
source.effective_end_datetime,
CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source and target.contact_source_system_nm='d3' then delete;

    
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for contact_adw_key

SELECT
     count(target.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`) source_FK_1
                          where target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.xref_contact_name. FK Column: contact_adw_key'));
 
  -- Orphaned foreign key check for nm_adw_key

SELECT
     count(target.nm_adw_key) AS nm_adw_key_count
 FROM
     (select distinct nm_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_name`)  target
       where not exists (select 1
                      from (select distinct nm_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_name`) source_FK_1
                          where target.nm_adw_key = source_FK_1.nm_adw_key)
HAVING
 IF((nm_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.xref_contact_name. FK Column: nm_adw_key'));

  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm, 
	nm_typ_cd, 
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_name`
  GROUP BY
    1,
    2,
	3,
	4
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.xref_contact_name' ));
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.contact_adw_key)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	nm_typ_cd, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_name`) a
JOIN (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	nm_typ_cd, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_name`) b
ON
  a.contact_adw_key=b.contact_adw_key 
  AND a.contact_source_system_nm=b.contact_source_system_nm 
  AND a.nm_typ_cd=b.nm_typ_cd
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.contact_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.xref_contact_name' ))
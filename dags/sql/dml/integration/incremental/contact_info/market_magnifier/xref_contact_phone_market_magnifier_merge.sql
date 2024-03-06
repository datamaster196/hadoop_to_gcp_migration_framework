create or replace table `{{ var.value.INTEGRATION_PROJECT }}.adw_work.market_magnifier_contact_phone_stage` as
with combined_cust_phone as
(
select contact_adw_key,
phone_adw_key,
cast(last_upd_dt as datetime) as last_upd_dt
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.market_magnifier_contact_stage`
union distinct
select
target.contact_adw_key,
target.phone_adw_key,
target.effective_start_datetime as last_upd_dt
from `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_phone` target
where contact_source_system_nm='market_magnifier'
 and phone_typ_cd like 'home%'
),
cust_phone as
(select
contact_adw_key,
phone_adw_key,
cast(last_upd_dt as datetime) as effective_start_datetime,
lag(phone_adw_key) over (partition by contact_adw_key order by cast(last_upd_dt as datetime), phone_adw_key) as prev_phone,
coalesce(lead(cast(last_upd_dt as datetime)) over (partition by contact_adw_key order by cast(last_upd_dt as datetime), phone_adw_key), datetime('9999-12-31'
) )as next_record_date
from combined_cust_phone
),
set_grouping_column as
(select
contact_adw_key,
phone_adw_key,
case when prev_phone is null or prev_phone<>phone_adw_key then 1 else 0 end as new_phone_tag,
effective_start_datetime,
next_record_date
from cust_phone
), set_groups as
(select
contact_adw_key,
phone_adw_key,
sum(new_phone_tag) over (partition by contact_adw_key order by effective_start_datetime, phone_adw_key) as grouping_column,
effective_start_datetime,
next_record_date
from set_grouping_column
), deduped as
(
select
contact_adw_key,
phone_adw_key,
grouping_column,
min(effective_start_datetime) as effective_start_datetime,
max(next_record_date) as effective_end_datetime
from set_groups
group by
contact_adw_key,
phone_adw_key,
grouping_column
),
update_key_typ_nm as
(
select
contact_adw_key,
phone_adw_key,
effective_start_datetime,
case when effective_end_datetime=datetime('9999-12-31') then effective_end_datetime else datetime_sub(effective_end_datetime, interval 1 second) end as effective_end_datetime,
row_number() over(partition by contact_adw_key,  effective_start_datetime order by effective_end_datetime desc,phone_adw_key ) as rn
from deduped
)
select
contact_adw_key,
phone_adw_key,
'market_magnifier' as source_system,
concat('home', case when rn=1 then '' else concat('-',cast(rn as string)) end ) as phone_type,
effective_start_datetime,
effective_end_datetime,
current_datetime insert_datetime,
{{ dag_run.id }} as  batch_insert,
current_datetime update_datetime,
{{ dag_run.id }} as  batch_update
from update_key_typ_nm
;
merge into `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_phone` target
using `{{ var.value.INTEGRATION_PROJECT }}.adw_work.market_magnifier_contact_phone_stage` source
on target.contact_adw_key=source.contact_adw_key and 
   target.contact_source_system_nm=source.source_system and
   target.phone_typ_cd=source.phone_type and
   target.effective_start_datetime=source.effective_start_datetime
when matched and (target.effective_end_datetime!=source.effective_end_datetime or target.phone_adw_key!=source.phone_adw_key) then update
set 
target.phone_adw_key=source.phone_adw_key,
target.effective_end_datetime=source.effective_end_datetime,
target.integrate_update_datetime=source.update_datetime,
target.integrate_update_batch_number=source.batch_update,
target.actv_ind=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END
when not matched by target then insert
(
contact_adw_key,    
phone_adw_key,    
contact_source_system_nm,
phone_typ_cd,    
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
source.phone_adw_key,
source.source_system,
source.phone_type,
source.effective_start_datetime,
source.effective_end_datetime,
CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y'  ELSE 'N'END,
source.insert_datetime,
source.batch_insert,
source.update_datetime,
source.batch_update
)
when not matched by source and target.contact_source_system_nm='market_magnifier' then delete;

    
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for contact_adw_key

SELECT
     count(target.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_phone`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`) source_FK_1
                          where target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.xref_contact_phone. FK Column: contact_adw_key'));
 
  -- Orphaned foreign key check for phone_adw_key

SELECT
     count(target.phone_adw_key) AS phone_adw_key_count
 FROM
     (select distinct phone_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_phone`)  target
       where not exists (select 1
                      from (select distinct phone_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_phone`) source_FK_1
                          where target.phone_adw_key = source_FK_1.phone_adw_key)
HAVING
 IF((phone_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.xref_contact_phone. FK Column: phone_adw_key'));

  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm, 
	phone_typ_cd, 
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_phone`
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
    ERROR( 'Error: Duplicate Records check failed for adw.xref_contact_phone' ));
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.contact_adw_key)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	phone_typ_cd, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_phone`) a
JOIN (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	phone_typ_cd, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_phone`) b
ON
  a.contact_adw_key=b.contact_adw_key 
  AND a.contact_source_system_nm=b.contact_source_system_nm 
  AND a.phone_typ_cd=b.phone_typ_cd
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.contact_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.xref_contact_phone' ))
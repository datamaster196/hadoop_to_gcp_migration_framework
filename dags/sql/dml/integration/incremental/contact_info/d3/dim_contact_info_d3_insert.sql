insert into `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`
(contact_adw_key ,
contact_typ_cd,    
contact_gender_cd,    
contact_gender_nm,    
contact_birth_dt,
adw_row_hash,
integrate_insert_datetime,    
integrate_insert_batch_number,    
integrate_update_datetime,    
integrate_update_batch_number
)
select
contact_adw_key ,
'Person' as contact_typ_cd,
max(gender) as contact_gender_cd,
cast(null as string) as contact_gender_nm,
max(birthdate) as birth_date,
'-1' as adw_row_hash,--default initial insert
current_datetime,
{{ dag_run.id }} as  batch_insert,
current_datetime,
{{ dag_run.id }} as  batch_update
from `{{ var.value.INTEGRATION_PROJECT }}.adw_work.d3_contact_stage`
where contact_adw_key not in (select distinct contact_adw_key from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`)
group by contact_adw_key;

    
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
-- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    contact_adw_key, 
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`
  GROUP BY
    1
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw_pii.dim_contact_info' )) 

MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_stage` b
ON
  (CONCAT(a.contact_adw_key,COALESCE(a.topic_nm,''),a.channel) = CONCAT(b.contact_adw_key,COALESCE(b.topic_nm,''),b.channel)
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
  contact_pref_adw_key,
contact_adw_key,
topic_nm,
channel,
event_cd,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
 )
 VALUES (
b.contact_pref_adw_key,
b.contact_adw_key,
b.topic_nm,
b.channel,
b.event_cd,
b.effective_start_datetime,
b.effective_end_datetime,
b.actv_ind,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number
)
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.actv_ind = b.actv_ind,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number;

  --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for contact_adw_key

SELECT
     count(target.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_contact_event_optout`) source_FK_1
                          where target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_contact_optout. FK Column: contact_adw_key'));
 
----------------------------------------------------------------------------------------------
-- Duplicate Checks

 select count(1)
   from
   (select CONCAT(contact_adw_key,COALESCE( topic_nm,''),channel) , effective_start_datetime, count(*) as dupe_count
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout`
   group by 1, 2
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_contact_optout'  ) );

 ---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a.contact_pref_adw_key ) from
 (select contact_pref_adw_key , CONCAT(contact_adw_key,COALESCE( topic_nm,''),channel) as contact_pref_source_key, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout`) a
 join
 (select contact_pref_adw_key , CONCAT(contact_adw_key,COALESCE( topic_nm,''),channel) as contact_pref_source_key, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout`) b
 on a.contact_pref_adw_key =b.contact_pref_adw_key
    and a.contact_pref_source_key  = b.contact_pref_source_key
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.contact_pref_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_contact_optout' ));

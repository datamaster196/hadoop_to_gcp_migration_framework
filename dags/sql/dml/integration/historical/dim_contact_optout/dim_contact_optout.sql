-- Prepare for a full refresh of data in each run
DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout` where 1=1;

-- Load all available data
INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout` ( contact_pref_adw_key,
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
    integrate_update_batch_number )
SELECT
  contact_pref_adw_key,
  source.contact_adw_key,
  source.topic_nm,
  source.channel,
  source.event_cd,
  CAST(hist_type_2.effective_start_datetime AS datetime) as effective_start_datetime ,
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_transformed` source
ON
  (CONCAT(source.contact_adw_key,COALESCE(source.topic_nm,''), source.channel) = hist_type_2.contact_pref_source_key
    AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS contact_pref_adw_key,
    CONCAT(contact_adw_key,COALESCE(topic_nm,''),channel) AS contact_pref_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_contact_optout_work_transformed`
  GROUP BY
    contact_adw_key,
    topic_nm,
    channel ) pk
ON
  (CONCAT( source.contact_adw_key,COALESCE(source.topic_nm,''),source.channel) =pk.contact_pref_source_key)
  ;
    
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
   (select CONCAT(contact_adw_key,COALESCE( topic_nm,''),channel),event_cd , effective_start_datetime, count(*) as dupe_count
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_optout`
   group by 1, 2, 3
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
  
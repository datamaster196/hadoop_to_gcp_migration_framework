CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_source`  as
SELECT
  CASE WHEN (b.email is NULL OR b.email='')
       THEN '-1'
       ELSE `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_email`(b.email)
       END as active_directory_email_adw_key ,
    CASE WHEN (a.email is NULL OR a.email='')
       THEN '-1'
       ELSE `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_email`(a.email) 
       END as email_adw_key ,
    CASE WHEN (workphone is NULL OR workphone='')
       THEN '-1'
       ELSE `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(workphone) 
       END as phone_adw_key ,
    CASE WHEN (faxphone is NULL OR faxphone='')
       THEN '-1'
       ELSE `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(faxphone) 
       END as fax_phone_adw_key ,
    CASE WHEN ((a.firstname IS NULL AND a.lastname IS NULL ) OR (a.firstname='' AND a.lastname=''))
        THEN '-1'
        ELSE `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(a.firstname,'',a.lastname,'','')
        END as nm_adw_key,
  'E' AS emp_typ_cd,
  b.loginid,
  b.email AS active_dir_email,
  COALESCE(b.flex_id,'UNKNOWN') AS flex_id,
  dateoriginalhire,
  terminationdate,
  eetermwhy,
  status,
  region,
  a.location,
  a.title,
  supervisor,
  faxphone,
  workphone,
  a.firstname,
  a.lastname,
  a.email AS email,
  ejjobcode,
  wd_costcenter,
  position_time_type,
  CAST('1900-01-01' AS datetime) as last_upd_dt,
  ROW_NUMBER() OVER (PARTITION BY b.loginid ORDER BY b.adw_lake_insert_datetime DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.epic.globallist` b
  LEFT JOIN
    `{{ var.value.INGESTION_PROJECT }}.epic.hr_sqldata` a
 ON
    a.flxId=b.flex_id

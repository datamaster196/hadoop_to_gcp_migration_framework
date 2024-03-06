INSERT INTO
   `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone` (phone_adw_key,
     raw_phone_nbr,
     cleansed_ph_nbr,
     cleansed_ph_area_cd_nbr,
     cleansed_ph_prefix_nbr,
     cleansed_ph_suffix_nbr,
     cleansed_ph_extension_nbr,
     phone_valid_status_cd,
     phone_valid_typ,
     integrate_insert_datetime,
     integrate_insert_batch_number,
     integrate_update_datetime,
     integrate_update_batch_number )
 SELECT
   DISTINCT phone_adw_key AS phone_adw_key,
   workphone AS raw_phone_nbr,
   '' AS cleansed_ph_nbr,
   CAST(NULL AS int64) cleansed_ph_area_cd_nbr,
   CAST(NULL AS int64) cleansed_ph_prefix_nbr,
   CAST(NULL AS int64) cleansed_ph_suffix_nbr,
   CAST(NULL AS int64) cleansed_ph_extension_nbr,
   'unverified' phone_valid_status_cd,
   'basic cleansing' phone_valid_typ,
   current_datetime,
   {{ dag_run.id }} AS integrate_insert_batch_number,
   current_datetime,
   {{ dag_run.id }} AS integrate_update_batch_number
 FROM
   `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed`
 WHERE
   workphone IS NOT NULL
   AND
   phone_adw_key NOT IN (
   SELECT
     phone_adw_key
   FROM
     `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone`)

 UNION DISTINCT

 SELECT
   DISTINCT fax_phone_adw_key AS phone_adw_key,
   faxphone AS raw_phone_nbr,
   '' AS cleansed_ph_nbr,
   CAST(NULL AS int64) cleansed_ph_area_cd_nbr,
   CAST(NULL AS int64) cleansed_ph_prefix_nbr,
   CAST(NULL AS int64) cleansed_ph_suffix_nbr,
   CAST(NULL AS int64) cleansed_ph_extension_nbr,
   'unverified' phone_valid_status_cd,
   'basic cleansing' phone_valid_typ,
   current_datetime,
   {{ dag_run.id }} AS integrate_insert_batch_number,
   current_datetime,
   {{ dag_run.id }} AS integrate_update_batch_number
 FROM
   `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_employee_work_transformed`
 WHERE
   faxphone IS NOT NULL
   AND
   fax_phone_adw_key NOT IN (
   SELECT
     phone_adw_key
   FROM
     `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_phone`)

CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.epic_contact_source_key_stage` AS
WITH
  normalized_source_key AS (
  SELECT
    contact_adw_key,
    'uniqentity_key' AS key_typ_nm,
    CAST(last_upd_dt AS datetime) last_upd_dt,
    client_unique_entity AS source_1_key,
    CAST(NULL AS string) AS source_2_key,
    CAST(NULL AS string) AS source_3_key,
    CAST(NULL AS string) AS source_4_key,
    CAST(NULL AS string) AS source_5_key,
    client_unique_entity AS dedupe_check
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.epic_contact_stage`
  UNION ALL
  SELECT
    contact_adw_key,
    'member_id' AS key_typ_nm,
    CAST(last_upd_dt AS datetime) last_upd_dt,
    CAST(iso_cd AS string) AS source_1_key,
    club_cd AS source_2_key,
    membership_id AS source_3_key,
    associate_id AS source_4_key,
    check_digit_nr AS source_5_key,
    CONCAT(ifnull(CAST(iso_cd AS string),
        ''),ifnull(club_cd,
        ''),ifnull(membership_id,
        ''),ifnull(associate_id,
        ''),ifnull(check_digit_nr,
        '')) AS dedupe_check
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.epic_contact_stage` ),
  all_keys AS (
  SELECT
    contact_adw_key,
    CASE
      WHEN key_typ_nm LIKE 'uniqentity_key%' THEN 'uniqentity_key'
      WHEN key_typ_nm LIKE 'member_id%' THEN 'member_id'
  END
    AS key_typ_nm,
    effective_start_datetime AS last_upd_dt,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    CASE
      WHEN key_typ_nm LIKE 'member_id%' THEN CONCAT(ifnull(CAST(source_1_key AS string), ''), ifnull(source_2_key, ''), ifnull(source_3_key, ''), ifnull(source_4_key, ''), ifnull(source_5_key, ''))
    ELSE
    source_1_key
  END
    AS dedupe_check
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`
  WHERE
    contact_source_system_nm = 'epic'
  UNION DISTINCT
  SELECT
    *
  FROM
    normalized_source_key ),
  cust_keys AS (
  SELECT
    contact_adw_key,
    key_typ_nm,
    last_upd_dt AS effective_start_datetime,
    LAG(dedupe_check) OVER (PARTITION BY contact_adw_key, key_typ_nm ORDER BY last_upd_dt, dedupe_check) AS prev_dedupe_check,
    coalesce(LEAD(last_upd_dt) OVER (PARTITION BY contact_adw_key, key_typ_nm ORDER BY last_upd_dt, dedupe_check),
      DATETIME('9999-12-31' ) )AS next_record_date,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    dedupe_check
  FROM
    all_keys ),
  set_grouping_column AS (
  SELECT
    contact_adw_key,
    key_typ_nm,
    CASE
      WHEN dedupe_check IS NULL OR prev_dedupe_check<>dedupe_check THEN 1
    ELSE
    0
  END
    AS new_key_tag,
    effective_start_datetime,
    next_record_date,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    dedupe_check
  FROM
    cust_keys ),
  set_groups AS (
  SELECT
    contact_adw_key,
    key_typ_nm,
    SUM(new_key_tag) OVER (PARTITION BY contact_adw_key, key_typ_nm ORDER BY effective_start_datetime, dedupe_check) AS grouping_column,
    effective_start_datetime,
    next_record_date,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key
  FROM
    set_grouping_column ),
  deduped AS (
  SELECT
    contact_adw_key,
    key_typ_nm,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_date) AS effective_end_datetime,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key
  FROM
    set_groups
  GROUP BY
    contact_adw_key,
    key_typ_nm,
    grouping_column,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key ),
  update_key_typ_nm AS (
  SELECT
    contact_adw_key,
    'epic' AS contact_source_system_nm,
    key_typ_nm,
    effective_start_datetime,
    CASE
      WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE
    datetime_sub(effective_end_datetime,
      INTERVAL 1 second)
  END
    AS effective_end_datetime,
    source_1_key,
    source_2_key,
    source_3_key,
    source_4_key,
    source_5_key,
    ROW_NUMBER() OVER(PARTITION BY contact_adw_key, key_typ_nm, effective_start_datetime ORDER BY effective_end_datetime DESC, source_1_key, source_2_key, source_3_key, source_4_key, source_5_key) AS rn
  FROM
    deduped )
SELECT
  contact_adw_key,
  contact_source_system_nm,
  CASE
    WHEN rn=1 THEN key_typ_nm
  ELSE
  CONCAT(key_typ_nm,'-',CAST(rn AS string))
END
  AS key_typ_nm,
  effective_start_datetime,
  effective_end_datetime,
  source_1_key,
  source_2_key,
  source_3_key,
  source_4_key,
  source_5_key,
  current_datetime insert_datetime,
  {{ dag_run.id }} AS batch_insert,
  current_datetime update_datetime,
  {{ dag_run.id }} AS batch_update,
  TO_BASE64(MD5(CONCAT(ifnull(contact_source_system_nm,
          ''),'|',ifnull(CASE
            WHEN rn=1 THEN key_typ_nm
          ELSE
          CONCAT(key_typ_nm,'-',CAST(rn AS string))
        END
          ,
          ''),'|',ifnull(source_1_key,
          ''),'|',ifnull(source_2_key,
          ''),'|',ifnull(source_3_key,
          ''),'|',ifnull(source_4_key,
          ''),'|',ifnull(source_5_key,
          '') ))) AS adw_row_hash
FROM
  update_key_typ_nm AS prep_stage
;
MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` target
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.epic_contact_source_key_stage` source
ON
  target.contact_adw_key=source.contact_adw_key
  AND target.contact_source_system_nm=source.contact_source_system_nm
  AND target.key_typ_nm=source.key_typ_nm
  AND target.effective_start_datetime=source.effective_start_datetime
  WHEN MATCHED AND (target.effective_end_datetime!=source.effective_end_datetime OR target.adw_row_hash!=source.adw_row_hash) THEN UPDATE 
  SET target.source_1_key=source.source_1_key, 
      target.source_2_key=source.source_2_key, 
	  target.source_3_key=source.source_3_key, 
	  target.source_4_key=source.source_4_key, 
	  target.source_5_key=source.source_5_key, 
	  target.adw_row_hash=source.adw_row_hash, 
	  target.effective_end_datetime=source.effective_end_datetime, 
	  target.integrate_update_datetime=source.update_datetime, 
	  target.integrate_update_batch_number=source.batch_update, 
	  target.actv_ind=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END
  WHEN NOT MATCHED BY TARGET THEN INSERT 
  ( contact_adw_key, 
    contact_source_system_nm, 
	key_typ_nm, 
	effective_start_datetime, 
	effective_end_datetime, 
	actv_ind, adw_row_hash, 
	source_1_key, 
	source_2_key, 
	source_3_key, 
	source_4_key, 
	source_5_key, 
	integrate_insert_datetime, 
	integrate_insert_batch_number, 
	integrate_update_datetime, 
	integrate_update_batch_number 
  ) 
  VALUES 
  ( source.contact_adw_key, 
    source.contact_source_system_nm, 
	source.key_typ_nm, 
	source.effective_start_datetime, 
	source.effective_end_datetime, 
	CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END,
    adw_row_hash, 
	source.source_1_key, 
	source.source_2_key, 
	source.source_3_key, 
	source.source_4_key, 
	source.source_5_key, 
	source.insert_datetime, 
	source.batch_insert, 
	source.update_datetime, 
	source.batch_update 
	)
  WHEN NOT MATCHED BY SOURCE AND target.contact_source_system_nm='epic' THEN
  DELETE
;

    
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for contact_adw_key

SELECT
     count(target.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`) source_FK_1
                          where target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_contact_source_key. FK Column: contact_adw_key'));
 
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm, 
	key_typ_nm, 
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_contact_source_key`
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
    ERROR( 'Error: Duplicate Records check failed for adw.dim_contact_source_key' ));
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.contact_adw_key)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	key_typ_nm, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_contact_source_key`) a
JOIN (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	key_typ_nm, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_contact_source_key`) b
ON
  a.contact_adw_key=b.contact_adw_key 
  AND a.contact_source_system_nm=b.contact_source_system_nm 
  AND a.key_typ_nm=b.key_typ_nm
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.contact_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_contact_source_key' ))


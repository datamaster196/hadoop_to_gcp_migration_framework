CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_address_stage` AS
WITH
  combined_cust_address AS (
  SELECT
    contact_adw_key,
    address_adw_key,
    CAST(last_upd_dt AS datetime) AS last_upd_dt
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_stage`
  UNION DISTINCT
  SELECT
    target.contact_adw_key,
    target.address_adw_key,
    target.effective_start_datetime AS last_upd_dt
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address` target
  WHERE
    contact_source_system_nm='mzp'
    AND address_typ_cd LIKE 'home%' ),
  cust_address AS (
  SELECT
    contact_adw_key,
    address_adw_key,
    CAST(last_upd_dt AS datetime) AS effective_start_datetime,
    LAG(address_adw_key) OVER (PARTITION BY contact_adw_key ORDER BY CAST(last_upd_dt AS datetime), address_adw_key) AS prev_address,
    coalesce(LEAD(CAST(last_upd_dt AS datetime)) OVER (PARTITION BY contact_adw_key ORDER BY CAST(last_upd_dt AS datetime), address_adw_key),
      datetime('9999-12-31' ) )AS next_record_date
  FROM
    combined_cust_address ),
  set_grouping_column AS (
  SELECT
    contact_adw_key,
    address_adw_key,
    CASE
      WHEN prev_address IS NULL OR prev_address<>address_adw_key THEN 1
    ELSE
    0
  END
    AS new_address_tag,
    effective_start_datetime,
    next_record_date
  FROM
    cust_address ),
  set_groups AS (
  SELECT
    contact_adw_key,
    address_adw_key,
    SUM(new_address_tag) OVER (PARTITION BY contact_adw_key ORDER BY effective_start_datetime, address_adw_key) AS grouping_column,
    effective_start_datetime,
    next_record_date
  FROM
    set_grouping_column ),
  deduped AS (
  SELECT
    contact_adw_key,
    address_adw_key,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_date) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    contact_adw_key,
    address_adw_key,
    grouping_column ),
  update_key_typ_nm AS (
  SELECT
    contact_adw_key,
    address_adw_key,
    effective_start_datetime,
    CASE
      WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE
    datetime_sub(effective_end_datetime,
      INTERVAL 1 second)
  END
    AS effective_end_datetime,
    ROW_NUMBER() OVER(PARTITION BY contact_adw_key, effective_start_datetime ORDER BY effective_end_datetime DESC, address_adw_key ) AS rn
  FROM
    deduped )
SELECT
  contact_adw_key,
  address_adw_key,
  'mzp' AS source_system,
  CONCAT('home',
    CASE
      WHEN rn=1 THEN ''
    ELSE
    CONCAT('-',CAST(rn AS string))
  END
    ) AS address_type,
  effective_start_datetime,
  effective_end_datetime,
  current_datetime insert_datetime,
  {{ dag_run.id }} AS batch_insert,
  current_datetime update_datetime,
  {{ dag_run.id }} AS batch_update
FROM
  update_key_typ_nm
;

MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address` target
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_address_stage` source
ON
  target.contact_adw_key=source.contact_adw_key
  AND target.contact_source_system_nm=source.source_system
  AND target.address_typ_cd=source.address_type
  AND target.effective_start_datetime=source.effective_start_datetime
  WHEN MATCHED AND (target.effective_end_datetime!=source.effective_end_datetime OR target.address_adw_key!=source.address_adw_key) THEN UPDATE 
  SET target.address_adw_key=source.address_adw_key, 
      target.effective_end_datetime=source.effective_end_datetime, 
	  target.integrate_update_datetime=source.update_datetime, 
	  target.integrate_update_batch_number=source.batch_update, 
	  target.actv_ind=CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END
  WHEN NOT MATCHED BY TARGET THEN INSERT 
  ( contact_adw_key, 
    address_adw_key, 
	contact_source_system_nm, 
	address_typ_cd, 
	effective_start_datetime, 
	effective_end_datetime, 
	actv_ind, 
	integrate_insert_datetime, 
	integrate_insert_batch_number, 
	integrate_update_datetime, 
	integrate_update_batch_number 
  ) 
  VALUES 
  ( source.contact_adw_key, 
    source.address_adw_key, 
	source.source_system, 
	source.address_type, 
	source.effective_start_datetime, 
	source.effective_end_datetime, 
	CASE WHEN source.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END,
    source.insert_datetime, 
	source.batch_insert, 
	source.update_datetime, 
	source.batch_update 
  )
  WHEN NOT MATCHED BY SOURCE AND target.contact_source_system_nm='mzp' THEN
  DELETE
 ;

    
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for contact_adw_key

SELECT
     count(target.contact_adw_key) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`) source_FK_1
                          where target.contact_adw_key = source_FK_1.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.xref_contact_address. FK Column: contact_adw_key'));
 
  -- Orphaned foreign key check for address_adw_key

SELECT
     count(target.address_adw_key) AS address_adw_key_count
 FROM
     (select distinct address_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address`)  target
       where not exists (select 1
                      from (select distinct address_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_address`) source_FK_1
                          where target.address_adw_key = source_FK_1.address_adw_key)
HAVING
 IF((address_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.xref_contact_address. FK Column: address_adw_key'));

  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm, 
	address_typ_cd, 
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_address`
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
    ERROR( 'Error: Duplicate Records check failed for adw.xref_contact_address' ));
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.contact_adw_key)
FROM (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	address_typ_cd, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_address`) a
JOIN (
  SELECT
    contact_adw_key, 
	contact_source_system_nm,  
	address_typ_cd, 
	effective_start_datetime, 
	effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.xref_contact_address`) b
ON
  a.contact_adw_key=b.contact_adw_key 
  AND a.contact_source_system_nm=b.contact_source_system_nm 
  AND a.address_typ_cd=b.address_typ_cd
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.contact_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.xref_contact_address' ))
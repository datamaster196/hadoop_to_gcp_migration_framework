MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.insurance_policy_line_work_stage` b
ON
  (a.ins_policy_line_source_key = b.ins_policy_line_source_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT ( 
ins_line_adw_key,
ofc_aca_adw_key,
premium_paid_vendor_key,
legal_issuing_vendor_key,
product_adw_key,
ins_policy_adw_key,
ins_policy_line_source_key,
ins_line_comm_percent,
ins_line_delivery_method_cd,
ins_line_delivery_method_desc,
ins_line_effective_dt,
ins_line_Expiration_dt,
ins_line_first_written_dt,
ins_line_status,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number ) 
VALUES (
 b.ins_line_adw_key,
b.ofc_aca_adw_key,
b.premium_paid_vendor_key,
b.legal_issuing_vendor_key,
b.product_adw_key,
b.ins_policy_adw_key,
b.ins_policy_line_source_key,
b.ins_line_comm_percent,
b.ins_line_delivery_method_cd,
b.ins_line_delivery_method_desc,
b.ins_line_effective_dt,
b.ins_line_Expiration_dt,
b.ins_line_first_written_dt,
b.ins_line_status,
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
-- Orphaned foreign key check for insurance_policy_line
SELECT
        count(target.ofc_aca_adw_key ) AS ofc_aca_adw_key
FROM
        (select distinct ofc_aca_adw_key
        from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`)  target
        where not exists (select 1
                         from (select distinct aca_office_adw_key
                               from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) source_FK_1
                               where target.ofc_aca_adw_key = source_FK_1.aca_office_adw_key)
HAVING
IF((ofc_aca_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy_line. FK Column: ofc_aca_adw_key'));

  -- Orphaned foreign key check for premium_paid_vendor_key
 SELECT
     count(target.premium_paid_vendor_key ) AS premium_paid_vendor_key_count
 FROM
     (select distinct premium_paid_vendor_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`)  target
       where not exists (select 1
                      from (select distinct vendor_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor`) source_FK_2
                          where target.premium_paid_vendor_key = source_FK_2.vendor_adw_key)
HAVING
 IF((premium_paid_vendor_key_count = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy_line. FK Column: premium_paid_vendor_key'));
 
 
  -- Orphaned foreign key check for legal_issuing_vendor_key
 SELECT
     count(target.legal_issuing_vendor_key ) AS legal_issuing_vendor_key_count
 FROM
     (select distinct legal_issuing_vendor_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`)  target
       where not exists (select 1
                      from (select distinct vendor_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor`) source_FK_3
                          where target.legal_issuing_vendor_key = source_FK_3.vendor_adw_key)
HAVING
 IF((legal_issuing_vendor_key_count = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy_line. FK Column: legal_issuing_vendor_key'));
 
 
  -- Orphaned foreign key check for product_adw_key
 SELECT
     count(target.product_adw_key ) AS product_adw_key_count
 FROM
     (select distinct product_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`)  target
       where not exists (select 1
                      from (select distinct product_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`) source_FK_4
                          where target.product_adw_key = source_FK_4.product_adw_key)
HAVING
 IF((product_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy_line. FK Column: product_adw_key'));
 
   -- Orphaned foreign key check for ins_policy_adw_key
 SELECT
     count(target.ins_policy_adw_key ) AS ins_policy_adw_key
 FROM
     (select distinct ins_policy_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`)  target
       where not exists (select 1
                      from (select distinct ins_policy_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy`) source_FK_5
                          where target.ins_policy_adw_key = source_FK_5.ins_policy_adw_key
                          ) AND ins_policy_adw_key <>'-1'
HAVING
 IF((ins_policy_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.insurance_policy_line. FK Column: ins_policy_adw_key'));
 
 
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    ins_policy_line_source_key,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.insurance_policy_line' ));
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.ins_line_adw_key)
FROM (
  SELECT
    ins_line_adw_key,
    ins_policy_line_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`) a
JOIN (
  SELECT
    ins_line_adw_key,
    ins_policy_line_source_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.insurance_policy_line`) b
ON
  a. ins_line_adw_key =b.ins_line_adw_key
  AND a. ins_policy_line_source_key = b.ins_policy_line_source_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.ins_line_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.insurance_policy_line' ));
    

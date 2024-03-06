  MERGE INTO
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor` a
  USING
    `{{var.value.INTEGRATION_PROJECT}}.adw_work.vendor_work_stage` b
  ON
    (a.vendor_adw_key = b.vendor_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT (
    vendor_adw_key,
biz_line_adw_key,
address_adw_key,
email_adw_key,
vendor_cd,
vendor_name,
vendor_typ,
vendor_contact,
preferred_vendor,
vendor_status,
fleet_ind,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number)
VALUES
(
b.vendor_adw_key,
b.biz_line_adw_key,
b.address_adw_key,
b.email_adw_key,
b.vendor_cd,
b.vendor_name,
b.vendor_typ,
b.vendor_contact,
b.preferred_vendor,
b.vendor_status,
'U',
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
-- Orphaned foreign key check for dim_vendor
SELECT
        count(target.biz_line_adw_key ) AS biz_line_adw_key
FROM
        (select distinct biz_line_adw_key
        from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor`)  target
        where not exists (select 1
                         from (select distinct biz_line_adw_key
                               from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_business_line`) source_FK_1
                               where target.biz_line_adw_key = source_FK_1.biz_line_adw_key)
HAVING
IF((biz_line_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_vendor. FK Column: biz_line_adw_key'));

SELECT
       count(target.address_adw_key ) AS address_adw_key
FROM
         (select distinct address_adw_key
          from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor`)  target
           where not exists (select 1
                          from (select distinct address_adw_key
                                from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_address`) source_FK_1
                                where target.address_adw_key = source_FK_1.address_adw_key)
HAVING
IF((address_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_vendor. FK Column: address_adw_key'));

 SELECT
         count(target.email_adw_key ) AS email_adw_key
 FROM
         (select distinct email_adw_key
          from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor`)  target
          where not exists (select 1
                          from (select distinct email_adw_key
                                from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_email`) source_FK_1
                                where target.email_adw_key = source_FK_1.email_adw_key)
 HAVING
 IF((email_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_vendor. FK Column: email_adw_key'));

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
      (select vendor_cd , effective_start_datetime, count(*) as dupe_count
      from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor`
      group by 1, 2
      having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_vendor'  ) );

---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.vendor_adw_key ) from
   (select vendor_adw_key , vendor_cd , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor`) a
join
   (select vendor_adw_key, vendor_cd, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor`) b
on a.vendor_adw_key=b.vendor_adw_key
      and a.vendor_cd  = b.vendor_cd
      and a.effective_start_datetime  <= b.effective_end_datetime
      and b.effective_start_datetime  <= a.effective_end_datetime
      and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.vendor_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_vendor' ));
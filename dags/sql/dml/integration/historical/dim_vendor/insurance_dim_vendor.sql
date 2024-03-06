INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_vendor`
  (
vendor_adw_key,
biz_line_adw_key,
address_adw_key,
email_adw_key,
vendor_cd,
vendor_name,
vendor_typ,
--internal_facility_cd,
vendor_contact,
preferred_vendor,
vendor_status,
-- service_region,
-- service_region_desc,
fleet_ind,
-- aaa_facility,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
  )
SELECT
vendor_adw_key,
source.biz_line_adw_key,
source.address_adw_key,
'-1',
source.vendor_cd,
source.vendor_name,
source.vendor_typ,
source.vendor_contact,
source.preferred_vendor,
source.vendor_status,
'U',
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
(CASE WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END) ,
source.adw_row_hash,
CURRENT_DATETIME() ,
{{ dag_run.id }} ,
CURRENT_DATETIME() ,
{{ dag_run.id }}
    FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_transformed` source
  ON (
  source.vendor_cd =hist_type_2.vendor_cd
  AND source.effective_start_datetime =hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS vendor_adw_key,
    vendor_cd
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.vendor_work_transformed`
  GROUP BY
    vendor_cd) pk
ON (source.vendor_cd =pk.vendor_cd);

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

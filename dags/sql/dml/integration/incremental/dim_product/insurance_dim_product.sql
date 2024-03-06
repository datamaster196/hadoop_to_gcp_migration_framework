MERGE INTO
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product` a
USING
  `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_stage` b
ON
  (a.product_adw_key = b.product_adw_key
  AND a.effective_start_datetime = b.effective_start_datetime)
  WHEN NOT MATCHED THEN INSERT (
  product_adw_key ,
  product_category_adw_key,
  product_sku,
  product_sku_key,
  vendor_adw_key,
  product_sku_desc,
     product_sku_effective_dt,
    product_sku_expiration_dt,
    product_item_nbr_cd,
    product_item_desc,
    product_vendor_nm,
    product_service_category_cd,
    product_status_cd,
    product_dynamic_sku,
    product_dynamic_sku_desc,
    -- product_dynamic_sku_identifier ,
    product_unit_cost,
    effective_start_datetime,
  effective_end_datetime,
  actv_ind,
  integrate_insert_datetime,
  integrate_insert_batch_number,
  integrate_update_datetime,
  integrate_update_batch_number,
  adw_row_hash
  ) VALUES (
  b.product_adw_key ,
  b.product_category_adw_key,
  b.product_sku,
  b.product_sku_key,
  b.vendor_adw_key,
  b.product_sku_desc,
    b.product_sku_effective_dt,
  b.product_sku_expiration_dt,
  b.product_item_nbr_cd,
  b.product_item_desc,
  b.product_vendor_nm,
  b.product_service_category_cd,
  b.product_status_cd,
  b.product_dynamic_sku,
  b.product_dynamic_sku_desc,
 -- b.product_dynamic_sku_identifier ,
  b.product_unit_cost,
  b.effective_start_datetime,
  b.effective_end_datetime,
  b.actv_ind,
  b.integrate_insert_datetime,
  b.integrate_insert_batch_number,
  b.integrate_update_datetime,
  b.integrate_update_batch_number,
  b.adw_row_hash)
  WHEN MATCHED
  THEN
UPDATE
SET
  a.effective_end_datetime = b.effective_end_datetime,
  a.actv_ind = b.actv_ind,
  a.integrate_update_datetime = b.integrate_update_datetime,
  a.integrate_update_batch_number = b.integrate_update_batch_number ;



--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for dim_employee
SELECT
       count(target.product_category_adw_key ) AS product_category_adw_key
FROM
       (select distinct product_category_adw_key
       from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`)  target
       where not exists (select 1
                        from (select distinct product_category_adw_key
                              from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product_category`) source_FK_1
                              where target.product_category_adw_key = source_FK_1.product_category_adw_key)
  HAVING
  IF((product_category_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_product_category. FK Column: product_category_adw_key'));

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
     (SELECT
  product_sku,product_category_adw_key,
  COUNT(*) CNT
FROM
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`
WHERE
  actv_ind	='Y'
GROUP BY
  1,2
HAVING
  CNT>1) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_product'  ) );

---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check
SELECT
  COUNT(a.product_adw_key)
FROM (
  SELECT
    product_adw_key,
    product_sku,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) a
JOIN (
  SELECT
    product_adw_key,
    product_sku,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) b
ON
  a. product_adw_key =b.product_adw_key
  AND a. product_sku = b.product_sku
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING IF ((count(a.product_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_product' ));
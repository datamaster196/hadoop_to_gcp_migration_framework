INSERT INTO
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product` ( product_adw_key,
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
    adw_row_hash,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  product_adw_key,
  source.product_category_adw_key,
  source.product_sku,
  source.product_sku_key,
  source.vendor_adw_key,
  source.product_sku_desc,
  SAFE_CAST(source.product_sku_effective_dt AS DATE),
  SAFE_CAST(source.product_sku_expiration_dt AS DATE),
  source.product_item_nbr_cd,
  source.product_item_desc,
  source.product_vendor_nm,
  source.product_service_category_cd,
  source.product_status_cd,
  source.product_dynamic_sku,
  source.product_dynamic_sku_desc,
  -- source.product_dynamic_sku_identifier ,
  SAFE_CAST(source.product_unit_cost AS NUMERIC),
  source.adw_row_hash,
  CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_type_2_hist` hist_type_2
JOIN
  `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_transformed` source
ON
  ( source.product_sku=hist_type_2.product_sku and source.product_category_adw_key=hist_type_2.product_category_adw_key
    AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS product_adw_key,
    product_sku,product_category_Adw_key
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw_work.insurance_product_work_transformed`
  GROUP BY
    product_sku,product_category_Adw_key) pk
ON
  (source.product_sku=pk.product_sku and source.product_category_adw_key=pk.product_category_adw_key);



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
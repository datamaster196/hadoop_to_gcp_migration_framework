MERGE INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` a
USING
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_cx_codes_source_stage` b
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
  a.integrate_update_batch_number = b.integrate_update_batch_number;

--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for dim_product
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
IF((product_category_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_product. FK Column: product_category_adw_key'));

SELECT
          count(target.vendor_adw_key ) AS vendor_adw_key
FROM
         (select distinct vendor_adw_key
          from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`)  target
          where not exists (select 1
                           from (select distinct vendor_adw_key
                                 from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_vendor`) source_FK_1
                                 where target.vendor_adw_key = source_FK_1.vendor_adw_key)
HAVING
IF((vendor_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_product. FK Column: vendor_adw_key'));


--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
     (select product_sku_key , effective_start_datetime, count(*) as dupe_count
      from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`
      group by 1, 2
      having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_product'  ) );

---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a.product_adw_key ) from
    (select product_adw_key , product_sku_key , effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) a
join
    (select product_adw_key, product_sku_key, effective_start_datetime, effective_end_datetime from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product`) b
on a.product_adw_key=b.product_adw_key
       and a.product_sku_key  = b.product_sku_key
       and a.effective_start_datetime  <= b.effective_end_datetime
       and b.effective_start_datetime  <= a.effective_end_datetime
       and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.product_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_product' ));
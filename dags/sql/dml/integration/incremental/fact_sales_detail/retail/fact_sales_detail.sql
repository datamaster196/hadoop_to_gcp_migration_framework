  MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_detail_work_transformed` b
  ON
    (a.sales_header_adw_key = b.sales_header_adw_key and a.line_item_nbr = b.line_item_nbr
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT (
    fact_sales_detail_adw_key
    ,sales_header_adw_key
    ,product_adw_key
    ,biz_line_adw_key
    ,line_item_nbr
    ,item_cd
    ,item_desc
    ,item_category_cd
    ,item_category_desc
    ,quantity_sold
    ,unit_sale_Price
    ,unit_labor_cost
    ,unit_item_cost
    ,extended_sale_price
    ,extended_labor_cost
    ,extended_item_cost
    ,effective_start_datetime
    ,effective_end_datetime
    ,actv_ind
    ,adw_row_hash
    ,integrate_insert_datetime
    ,integrate_insert_batch_number
    ,integrate_update_datetime
    ,integrate_update_batch_number
   ) VALUES (
     b.fact_sales_detail_adw_key
    ,b.sales_header_adw_key
    ,b.product_adw_key
    ,b.biz_line_adw_key
    ,b.line_item_nbr
    ,b.item_cd
    ,b.item_desc
    ,b.item_category_cd
    ,b.item_category_desc
    ,b.quantity_sold
    ,b.unit_sale_Price
    ,b.unit_labor_cost
    ,b.unit_item_cost
    ,b.extended_sale_price
    ,b.extended_labor_cost
    ,b.extended_item_cost
    ,b.effective_start_datetime
    ,b.effective_end_datetime
    ,b.actv_ind
    ,b.adw_row_hash
    ,CURRENT_DATETIME()
    ,{{ dag_run.id }}
    ,CURRENT_DATETIME()
    ,{{ dag_run.id }}
  )
    WHEN MATCHED and a.adw_row_hash <> b.adw_row_hash
    THEN
  UPDATE
  SET
    a.product_adw_key             =  b.product_adw_key
    ,a.biz_line_adw_key            =  b.biz_line_adw_key
    ,a.item_cd                     =  b.item_cd
    ,a.item_desc                   =  b.item_desc
    ,a.item_category_cd            =  b.item_category_cd
    ,a.item_category_desc          =  b.item_category_desc
    ,a.quantity_sold               =  b.quantity_sold
    ,a.unit_sale_Price             =  b.unit_sale_Price
    ,a.unit_labor_cost             =  b.unit_labor_cost
    ,a.unit_item_cost              =  b.unit_item_cost
    ,a.extended_sale_price         =  b.extended_sale_price
    ,a.extended_labor_cost         =  b.extended_labor_cost
    ,a.extended_item_cost          =  b.extended_item_cost
    ,a.effective_start_datetime    =  b.effective_start_datetime
    ,a.effective_end_datetime      = b.effective_end_datetime
    ,a.actv_ind                    = b.actv_ind
    ,a.adw_row_hash                =  b.adw_row_hash
    ,a.integrate_update_datetime       = CURRENT_DATETIME()
    ,a.integrate_update_batch_number = {{ dag_run.id }};

      --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for address_adw_key

SELECT
     count(target.sales_header_adw_key) AS sales_header_adw_key_count
 FROM
     (select distinct sales_header_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`)  target
       where not exists (select 1
                      from (select distinct sales_header_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`) source_FK_1
                          where target.sales_header_adw_key = source_FK_1.sales_header_adw_key)
HAVING
 IF((sales_header_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.Fact_sales_header. FK Column: sales_header_adw_key'));

SELECT
     count(target.product_adw_key) AS product_adw_key_count
 FROM
     (select distinct product_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`)  target
       where not exists (select 1
                      from (select distinct product_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product`) source_FK_2
                          where target.product_adw_key = source_FK_2.product_adw_key)
HAVING
 IF((product_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_product. FK Column: product_adw_key'));

SELECT
     count(target.biz_line_adw_key) AS biz_line_adw_key_count
 FROM
     (select distinct biz_line_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`)  target
       where not exists (select 1
                      from (select distinct biz_line_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_business_line`) source_FK_3
                          where target.biz_line_adw_key = source_FK_3.biz_line_adw_key)
HAVING
 IF((biz_line_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_business_line. FK Column: biz_line_adw_key'));


----------------------------------------------------------------------------------------------
-- Duplicate Checks

 select count(1)
   from
   (select sales_header_adw_key ,line_item_nbr, effective_start_datetime, count(*) as dupe_count
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`
   group by 1, 2,3
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.fact_sales_detail'  ) );

 ---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a.fact_sales_detail_adw_key ) from
 (select fact_sales_detail_adw_key ,line_item_nbr, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`) a
 join
 (select fact_sales_detail_adw_key ,line_item_nbr, effective_start_datetime, effective_end_datetime from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_detail`) b
 on a.fact_sales_detail_adw_key  = b.fact_sales_detail_adw_key
    and a.line_item_nbr  = b.line_item_nbr
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.fact_sales_detail_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.fact_sales_detail' ));
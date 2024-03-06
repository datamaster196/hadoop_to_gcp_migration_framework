INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`
(
sales_header_adw_key,
aca_office_adw_key,
emp_adw_key,
contact_adw_key,
source_system,
source_system_key,
sale_adw_key_dt,
sale_dtm,
Receipt_nbr,
sale_Void_ind,
Return_ind,
total_sale_Price,
total_labor_cost,
total_cost,
total_tax,
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
sales_header_adw_key,
source.aca_office_adw_key,
source.employee_adw_key,
source.contact_adw_key,
source.source_system,
source.source_system_key,
source.sale_adw_key_dt,
SAFE_CAST(source.sale_dtm AS DATETIME),
source.Receipt_nbr,
source.sale_Void_ind,
source.Return_ind,
SAFE_CAST(source.total_sale_Price AS NUMERIC),
source.total_Labor_cost,
SAFE_CAST(source.total_cost AS NUMERIC),
SAFE_CAST(source.total_tax AS NUMERIC),
 SAFE_CAST(hist_type_2.effective_start_datetime AS datetime),
  CAST(hist_type_2.effective_end_datetime AS datetime),
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
    ELSE 'N'
  END AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_transformed` source ON (
  CONCAT('VAST',source. company ,source. source_system_key ) = hist_type_2.fact_sales_header_source_key
  AND source.effective_start_datetime=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS sales_header_adw_key,
    CONCAT('VAST', company , source_system_key ) fact_sales_header_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_transformed`
  GROUP BY
    company,
    source_system_key) pk
ON (CONCAT('VAST',source.company ,source. source_system_key ) =pk.fact_sales_header_source_key);


   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for aca_office_adw_key
SELECT
     count(target.aca_office_adw_key ) AS aca_office_adw_key_count
 FROM
     (select distinct aca_office_adw_key
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`) target
       where not exists (select 1
                      from (select distinct aca_office_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) source_FK_2
                          where target.aca_office_adw_key = source_FK_2.aca_office_adw_key)
HAVING
 IF((aca_office_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.fact_sales_header. FK Column: aca_office_adw_key '));
 
 -- Orphaned foreign key check for emp_adw_key

SELECT
  COUNT(target.emp_adw_key ) AS emp_adw_key_count
FROM (
  SELECT
    DISTINCT emp_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT emp_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee`) source_FK_2
  WHERE
    target.emp_adw_key = source_FK_2.emp_adw_key)
HAVING
IF
  ((emp_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.fact_sales_header. FK Column: emp_adw_key'));
    
-- Orphaned foreign key check for contact_adw_key
 SELECT
     count(target.contact_adw_key ) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`) source_FK_3
                          where target.contact_adw_key = source_FK_3.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.fact_sales_header. FK Column: contact_adw_key '));

  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks

 select count(1)
   from
   (select source_system,source_system_key , aca_office_adw_key, effective_start_datetime, count(*) as dupe_count -- source_system_key needs to be updated
   from `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`
   group by 1, 2, 3 ,4
   having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.fact_sales_header'  ) );

 ---------------------------------------------------------------------------------------------
 -- Effective Dates overlapping check

select count(a. sales_header_adw_key ) from
 (select sales_header_adw_key , source_system_key , aca_office_adw_key, effective_start_datetime, effective_end_datetime from  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`) a
 join
 (select sales_header_adw_key, source_system_key, aca_office_adw_key, effective_start_datetime, effective_end_datetime from  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`) b
 on a.sales_header_adw_key=b.sales_header_adw_key
    and a.source_system_key  = b.source_system_key
    and a.aca_office_adw_key  = b.aca_office_adw_key
    and a.effective_start_datetime  <= b.effective_end_datetime
    and b.effective_start_datetime  <= a.effective_end_datetime
    and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.sales_header_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.fact_sales_header' ));

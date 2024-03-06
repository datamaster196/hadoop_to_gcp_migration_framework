  -----source query
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_source` AS
SELECT 
MAX(fact_sales_header_source.consultant_id) as consultant_id,
MAX(fact_sales_header_source.cust_id) as cust_id,
TRIM(fact_sales_header_source.ofc_id) as ofc_id,
TRIM(fact_sales_header_source.sale_id) as sale_id,
"POS" as source_system,
fact_sales_header_source.sale_id as source_system_key,
'-1' as sale_adw_key_dt,
MAX(cust_vst_dt) as sale_dtm,
MAX(cust_recpt_nr) as Receipt_nbr,
CASE WHEN MAX(cust_recpt_void_ret_ind) ='V' THEN 'Y' 
     ELSE ''
     END as sale_Void_ind,
CASE WHEN MAX(cust_recpt_void_ret_ind) ='Y' THEN 'Y' 
     ELSE 'U'
     END as Return_ind,
MAX(cust_recpt_tot_sls_amt) as total_sale_Price,
0 as total_Labor_cost,
SUM(SAFE_CAST(cust_recpt_ln_itm.user_dec AS INT64))/100 as total_cost,
MAX(cust_recpt_tax_amt) as total_tax,
MAX(fact_sales_header_source.last_update) as effective_start_datetime,
ROW_NUMBER() OVER(PARTITION BY fact_sales_header_source.ofc_id,fact_sales_header_source.sale_id ORDER BY MAX(fact_sales_header_source.last_update) ) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.pos.cust_recpt` AS fact_sales_header_source
  left outer join `{{ var.value.INGESTION_PROJECT }}.pos.cust_recpt_ln_itm` cust_recpt_ln_itm 
  ON TRIM(fact_sales_header_source.ofc_id)=TRIM(cust_recpt_ln_itm.ofc_id)
  AND TRIM(fact_sales_header_source.sale_id)=TRIM(cust_recpt_ln_itm.sale_id)
    WHERE
   SAFE_CAST(fact_sales_header_source.last_update AS datetime) > (
   SELECT
    MAX(effective_start_datetime)
   FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`)
 group by fact_sales_header_source.ofc_id,fact_sales_header_source.sale_id;
  -----------------------------------------------------------------------------------------------
  -------------Transformations--------------------------------
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_transformed` AS
SELECT
  ofc_id,
  sale_id,
  COALESCE(office.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  COALESCE(employee.emp_adw_key,
    "-1") AS employee_adw_key,
  COALESCE(contact.contact_adw_key,
    "-1") AS contact_adw_key,
  source_system,
  source_system_key,
  sale_adw_key_dt,
  sale_dtm,
  Receipt_nbr,
  sale_Void_ind,
  Return_ind,
  total_sale_Price,
  total_Labor_cost,
  total_cost,
  total_tax,
  fact_sales_header_source.effective_start_datetime,
  TO_BASE64(MD5(CONCAT(ifnull(COALESCE(office.aca_office_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(employee.emp_adw_key,
            "-1"),
          ''), '|', ifnull(COALESCE(contact.contact_adw_key,
            "-1"),
          ''), '|', ifnull(fact_sales_header_source.sale_adw_key_dt,
          ''),'|', ifnull(fact_sales_header_source.sale_dtm,
          ''),'|', ifnull(fact_sales_header_source.Receipt_nbr,
          ''),'|', ifnull(fact_sales_header_source.sale_Void_ind,
          ''),'|', ifnull(fact_sales_header_source.Return_ind,
          ''),'|', ifnull(fact_sales_header_source.total_sale_Price,
          ''),'|', ifnull(SAFE_CAST(fact_sales_header_source.total_Labor_cost AS STRING),
          ''),'|', ifnull(SAFE_CAST(fact_sales_header_source.total_cost AS STRING),
          ''),'|', ifnull(fact_sales_header_source.total_Tax,
          ''),'|' ))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_source` AS fact_sales_header_source
LEFT OUTER JOIN
(select retail_sales_office_cd,
        aca_office_adw_key,
        actv_ind,
        row_number() over(partition by retail_sales_office_cd) dupe_check
from
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) office
ON
  SAFE_CAST(fact_sales_header_source.ofc_id AS INT64)=SAFE_CAST(office.retail_sales_office_cd AS INT64)
  AND office.actv_ind='Y' and office.dupe_check=1
LEFT OUTER JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` employee
ON
  employee.emp_active_directory_user_id=fact_sales_header_source.consultant_id
  AND employee.actv_ind='Y'
LEFT OUTER JOIN
 (SELECT
    contact_adw_key,
    source_1_key,
    contact_source_system_nm,
    actv_ind,
    key_typ_nm,
    ROW_NUMBER() OVER (PARTITION BY source_1_key) DUPE_CHECK
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key`
 WHERE
    contact_source_system_nm='pos'
    AND key_typ_nm LIKE '%cust_id%' ) contact
ON
  contact.source_1_key=fact_sales_header_source.cust_id
  AND contact.actv_ind ='Y'
  AND contact.DUPE_CHECK=1
WHERE
  fact_sales_header_source.dupe_check=1 ;
  --------------------------------------------------------------------------------------------------------------
  ------------------------------------Stage query---------------------------------------------------------------
CREATE OR REPLACE TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_stage` AS
SELECT 
COALESCE(target.sales_header_adw_key,
          TO_BASE64(MD5(concat(coalesce(trim(source.sale_id),''),
                       coalesce(trim(source.ofc_id),''),
					   'POS')))
					   ) sales_header_adw_key,
source.aca_office_adw_key,
source.employee_adw_key ,
source.contact_adw_key,
source.source_system,
source.source_system_key,
source.sale_adw_key_dt,
SAFE_CAST(source.sale_dtm AS DATETIME) AS sale_dtm,
source.receipt_nbr,
source.sale_void_ind,
source.return_ind,
SAFE_CAST(source.total_sale_price AS NUMERIC) AS total_sale_price,
SAFE_CAST(source.total_labor_cost AS NUMERIC) AS total_labor_cost,
SAFE_CAST(source.total_cost AS NUMERIC) AS total_cost,
SAFE_CAST(source.total_tax AS NUMERIC) AS total_tax,
 SAFE_CAST(source.effective_start_datetime as DATETIME) as effective_start_datetime ,
  CAST('9999-12-31' AS datetime) effective_end_datetime,
  'Y' AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME() integrate_insert_datetime,
  {{ dag_run.id }} integrate_insert_batch_number,
  CURRENT_DATETIME() integrate_update_datetime,
  {{ dag_run.id }} integrate_update_batch_number
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_transformed` source
LEFT JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header` target
ON
  (source.source_system=target.source_system and source.source_system_key=target.source_system_key AND source.aca_office_adw_key=target.aca_office_adw_key
    AND target.actv_ind='Y')
WHERE
  target.source_system_key IS NULL
  OR source.adw_row_hash <> target.adw_row_hash
  UNION ALL
  SELECT
  sales_header_adw_key,
  target.aca_office_adw_key,
target.emp_adw_key,
target.contact_adw_key,
target.source_system,
target.source_system_key,
target.sale_adw_key_dt,
target.sale_dtm,
target.receipt_nbr,
target.sale_void_ind,
target.return_ind,
target.total_sale_price,
target.total_labor_cost,
target.total_cost,
target.total_tax,
  target.effective_start_datetime,
  DATETIME_SUB(cast(source.effective_start_datetime as datetime),
    INTERVAL 1 second ) AS effective_end_datetime,
  'N' AS actv_ind,
  target.adw_row_hash,
  target.integrate_insert_datetime,
  target.integrate_insert_batch_number,
  CURRENT_DATETIME(),
  {{ dag_run.id }} integrate_update_batch_number
 FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header` target
ON
  (source.source_system=target.source_system and source.source_system_key=target.source_system_key AND source.aca_office_adw_key=target.aca_office_adw_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash;
  ----------------------------End of script-------------------------------------------------------
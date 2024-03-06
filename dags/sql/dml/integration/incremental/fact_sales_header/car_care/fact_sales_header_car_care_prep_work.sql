-----------------------------------------------------------------------------------------------
-----------------------------------source query------------------------------------------------
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_source` AS
SELECT
  TRIM(histhdr.company) AS company,
  TRIM(histhdr.estimate_number) AS source_system_key,
  '-1' AS sale_adw_key_dt,
  MAX(customer_number) AS customer_number,
  "VAST" AS source_system,
  MAX(invoice_date) AS sale_dtm,
  MAX(invoice_number) AS receipt_nbr,
  CASE
    WHEN SAFE_CAST(MAX(invoice_number) AS int64)=0 THEN 'Y'
  ELSE
  'U'
END
  AS sale_void_ind,
  CASE
    WHEN SAFE_CAST(MAX(total_sale_amount) AS int64) < 0 THEN 'Y'
  ELSE
  'U'
END
  AS return_ind,
  MAX(total_sale_amount) AS total_sale_price,
  (SAFE_CAST(MAX(histline.quantity) AS numeric)*SAFE_CAST(MAX(histline.sell_labor) AS numeric) + SAFE_CAST(MAX(histline.labor_cost) AS numeric)) AS total_labor_cost,
  (SAFE_CAST(MAX(histline.quantity) AS numeric)*SAFE_CAST(MAX(histline.cost) AS numeric) + SAFE_CAST(MAX(histline.labor_cost) AS numeric)) AS total_cost,
  MAX(total_taxable_amount) AS total_tax,
  MAX(DATESTAMP) as effective_start_datetime,
  ROW_NUMBER() OVER (PARTITION BY histhdr.company, histhdr.estimate_number ORDER BY  MAX(DATESTAMP) ) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.vast.histhdr` AS histhdr
LEFT OUTER JOIN (
  SELECT
   '' company,
   '' estimate_number,
   '' quantity,
   '' sell_labor,
   '' labor_cost,
   '' cost
      ) histline
ON
  histhdr.company=histline.company
  AND histhdr.estimate_number=histline.estimate_number
      WHERE
   SAFE_CAST(histhdr.DATESTAMP AS datetime) > (
   SELECT
    MAX(effective_start_datetime)
   FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`)
GROUP BY
  histhdr.company,
  histhdr.estimate_number;
 -----------------------------------------------------------------------------------------------
 ------------------------------------------------Transformations--------------------------------
  CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_transformed` AS
select * from (SELECT
  company,
  coalesce(office.aca_office_adw_key,
    '-1') AS aca_office_adw_key,
  '-1' AS employee_adw_key,
  coalesce(contact1.contact_adw_key,
    '-1') AS contact_adw_key,
  source_system,
  source_system_key,
  sale_adw_key_dt,
  sale_dtm,
  receipt_nbr,
  sale_void_ind,
  return_ind,
  total_sale_price,
  coalesce(total_labor_cost,
    0) AS total_labor_cost,
  coalesce(total_cost,
    0) AS total_cost,
  total_tax,
  fact_sales_header_source.effective_start_datetime,
  to_base64(md5(CONCAT(ifnull(coalesce(office.aca_office_adw_key,
            "-1"),
          ''), '|', ifnull(coalesce('-1',
            "-1"),
          ''), '|', ifnull(coalesce(contact1.contact_adw_key,
            "-1"),
          ''), '|', ifnull(fact_sales_header_source.sale_adw_key_dt,
          ''),'|', ifnull(fact_sales_header_source.sale_dtm,
          ''),'|', ifnull(fact_sales_header_source.receipt_nbr,
          ''),'|', ifnull(fact_sales_header_source.sale_void_ind,
          ''),'|', ifnull(fact_sales_header_source.return_ind,
          ''),'|', ifnull(fact_sales_header_source.total_sale_price,
          ''),'|', ifnull(SAFE_CAST(fact_sales_header_source.total_labor_cost AS string),
          ''),'|', ifnull(SAFE_CAST(fact_sales_header_source.total_cost AS string),
          ''),'|', ifnull(fact_sales_header_source.total_tax,
          ''),'|' ))) AS adw_row_hash,
          ROW_NUMBER() OVER(PARTITION BY 'VAST',coalesce(office.aca_office_adw_key,'-1'),source_system_key ORDER BY NULL DESC) AS dup_check
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_source` AS fact_sales_header_source
LEFT OUTER JOIN
(select car_care_loc_nbr,
        aca_office_adw_key,
        actv_ind,
        row_number() over(partition by car_care_loc_nbr) dupe_check
from
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`) office
ON
  fact_sales_header_source.company=office.car_care_loc_nbr
  AND actv_ind='Y' AND office.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    contact.contact_adw_key,
    header.customer_number,ROW_NUMBER() OVER (PARTITION BY source_1_key,source_2_key) DUPE_CHECK
  FROM
    `{{ var.value.INGESTION_PROJECT }}.vast.histhdr` header,
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_contact_source_key` contact
  WHERE
    contact.source_1_key=header.CUSTOMER_NUMBER
    AND contact.source_2_key=header.COMPANY
    AND contact.contact_source_system_nm='vast'
    AND contact.key_typ_nm LIKE 'vast_cust_nbr%'
    AND contact.actv_ind ='Y') contact1
ON
  fact_sales_header_source.customer_number =contact1.customer_number
WHERE
  fact_sales_header_source.dupe_check=1 and contact1.DUPE_CHECK=1
  )
 where dup_check=1  ;
  ---------------------------------------------------------------------------------------------------------
  -------------------------------------------Stage query---------------------------------------------
 CREATE OR REPLACE TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_stage` AS
SELECT 
COALESCE(target.sales_header_adw_key, GENERATE_UUID()
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_transformed` source
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
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_transformed` source
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header` target
ON
  (source.source_system=target.source_system and source.source_system_key=target.source_system_key AND source.aca_office_adw_key=target.aca_office_adw_key
    AND target.actv_ind='Y')
WHERE
  source.adw_row_hash <> target.adw_row_hash;
  ----------------------------End of script-------------------------------------------------------
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
  COALESCE(MAX(invoice_date),MAX(DATESTAMP)) as effective_start_datetime,
  ROW_NUMBER() OVER (PARTITION BY histhdr.company, histhdr.estimate_number, COALESCE(MAX(invoice_date),MAX(DATESTAMP)) ORDER BY NULL ) AS dupe_check
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
  -------------------------------------------Type 2 hist query---------------------------------------------
 CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_type_2_hist` AS
WITH
  fact_sales_header_pos_hist AS (
  SELECT
    CONCAT('VAST', company , source_system_key ) as fact_sales_header_source_key ,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY company,source_system_key ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(cast(effective_start_datetime as datetime)) OVER (PARTITION BY company,source_system_key ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_vast_work_transformed` 
  ), set_grouping_column AS (
  SELECT
    fact_sales_header_source_key ,
    adw_row_hash,
    CASE
      WHEN prev_row_hash IS NULL OR prev_row_hash<>adw_row_hash THEN 1
    ELSE
    0
  END
    AS new_record_tag,
    effective_start_datetime,
    next_record_datetime
  FROM
    fact_sales_header_pos_hist 
  ), set_groups AS (
  SELECT
    fact_sales_header_source_key ,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY fact_sales_header_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column 
  ), deduped AS (
  SELECT
    fact_sales_header_source_key ,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    fact_sales_header_source_key ,
    adw_row_hash,
    grouping_column 
  )
SELECT
  fact_sales_header_source_key ,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
    ELSE datetime_sub(effective_end_datetime, INTERVAL 1 second)
  END AS effective_end_datetime
FROM
  deduped;
  ----------------------------End of script-------------------------------------------------------
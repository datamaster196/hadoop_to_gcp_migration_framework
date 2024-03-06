  -----source query
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_source` AS
SELECT
  MAX(fact_sales_header_source.consultant_id) AS consultant_id,
  MAX(fact_sales_header_source.cust_id) AS cust_id,
  TRIM(fact_sales_header_source.ofc_id) AS ofc_id,
  TRIM(fact_sales_header_source.sale_id) AS sale_id,
  "POS" AS source_system,
  TRIM(fact_sales_header_source.sale_id) AS source_system_key,
  '-1' AS sale_adw_key_dt,
  MAX(cust_vst_dt) AS sale_dtm,
  MAX(cust_recpt_nr) AS Receipt_nbr,
  CASE
    WHEN MAX(cust_recpt_void_ret_ind) ='V' THEN 'Y'
  ELSE
  'U'
END
  AS sale_Void_ind,
  CASE
    WHEN MAX(cust_recpt_void_ret_ind) ='Y' THEN 'Y'
  ELSE
  'U'
END
  AS Return_ind,
  MAX(cust_recpt_tot_sls_amt) AS total_sale_Price,
  0 AS total_Labor_cost,
  SUM(SAFE_CAST(cust_recpt_ln_itm.user_dec AS INT64))/100 AS total_cost,
  MAX(cust_recpt_tax_amt) AS total_tax,
  COALESCE(MAX(fact_sales_header_source.cust_vst_dt),
    MAX(fact_sales_header_source.cust_vst_dt)) AS effective_start_datetime,
  ROW_NUMBER() OVER(PARTITION BY fact_sales_header_source.ofc_id, fact_sales_header_source.sale_id, COALESCE(MAX(fact_sales_header_source.cust_vst_dt),
      MAX(fact_sales_header_source.last_update))
  ORDER BY
    NULL ) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.pos.cust_recpt` AS fact_sales_header_source
LEFT OUTER JOIN
  `{{ var.value.INGESTION_PROJECT }}.pos.cust_recpt_ln_itm` cust_recpt_ln_itm
ON
  TRIM(fact_sales_header_source.ofc_id)=TRIM(cust_recpt_ln_itm.ofc_id)
  AND TRIM(fact_sales_header_source.sale_id)=TRIM(cust_recpt_ln_itm.sale_id)
GROUP BY
  fact_sales_header_source.ofc_id,
  fact_sales_header_source.sale_id;
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
  ----------------------------------------------------------------------------------------------------------------
  -------------------------Type 2 hist query---------------------------------------------
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_type_2_hist` AS
WITH
  fact_sales_header_pos_hist AS (
  SELECT
    CONCAT('POS',ofc_id,sale_id) AS fact_sales_header_source_key,
    adw_row_hash,
    effective_start_datetime,
    LAG(adw_row_hash) OVER (PARTITION BY ofc_id, sale_id ORDER BY effective_start_datetime) AS prev_row_hash,
    coalesce(LEAD(CAST(effective_start_datetime AS datetime)) OVER (PARTITION BY ofc_id, sale_id ORDER BY effective_start_datetime),
      datetime('9999-12-31' ) )AS next_record_datetime
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.fact_sales_header_pos_work_transformed` ),
  set_grouping_column AS (
  SELECT
    fact_sales_header_source_key,
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
    fact_sales_header_pos_hist ),
  set_groups AS (
  SELECT
    fact_sales_header_source_key,
    adw_row_hash,
    SUM(new_record_tag) OVER (PARTITION BY fact_sales_header_source_key ORDER BY effective_start_datetime) AS grouping_column,
    effective_start_datetime,
    next_record_datetime
  FROM
    set_grouping_column ),
  deduped AS (
  SELECT
    fact_sales_header_source_key,
    adw_row_hash,
    grouping_column,
    MIN(effective_start_datetime) AS effective_start_datetime,
    MAX(next_record_datetime) AS effective_end_datetime
  FROM
    set_groups
  GROUP BY
    fact_sales_header_source_key,
    adw_row_hash,
    grouping_column )
SELECT
  fact_sales_header_source_key,
  adw_row_hash,
  effective_start_datetime,
  CASE
    WHEN effective_end_datetime=datetime('9999-12-31') THEN effective_end_datetime
  ELSE
  datetime_sub(effective_end_datetime,
    INTERVAL 1 second)
END
  AS effective_end_datetime
FROM
  deduped;
  ----------------------------End of script-------------------------------------------------------
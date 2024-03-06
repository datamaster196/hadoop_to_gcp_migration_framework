INSERT INTO
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product_category` ( product_category_adw_key,
    biz_line_adw_key,
    product_category_cd,
    product_category_desc,
    product_category_status_cd,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number )
SELECT
  a.product_category_adw_key,
  a.biz_line_adw_key,
  a.product_category_cd,
  a.product_category_desc,
  a.product_category_status_cd,
  a.effective_start_datetime,
  a.effective_end_datetime,
  a.actv_ind,
  a.adw_row_hash,
  a.integrate_insert_datetime,
  a.integrate_insert_batch_number,
  a.integrate_update_datetime,
  a.integrate_update_batch_number
FROM (
  SELECT
    '574ad6f5-1233-42ff-bb2b-ebda5e204c31' AS product_category_adw_key,
    '33b05324-9a68-4775-9c89-eee81867b23d' AS biz_line_adw_key,
    'MBR_FEES' AS product_category_cd,
    'MEMBERSHIP FEES' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    '31RBTy8wt/gtNUVmsi10Bg==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    '48124364-3e3e-4194-8880-8ddcdfb5acc9' AS product_category_adw_key,
    '33b05324-9a68-4775-9c89-eee81867b23d' AS biz_line_adw_key,
    'RCC' AS product_category_cd,
    'RIDER COMPONENT CODES' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    '4Mtrot6F8t7eeByMhLBm9Q==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    'eefce1e6-3fee-4ba2-a19c-0e54b29abc72' AS product_category_adw_key,
    '33b05324-9a68-4775-9c89-eee81867b23d' AS biz_line_adw_key,
    'BA' AS product_category_cd,
    'BILL ADJUSTMENT' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    'Ca6lNXL0Gly/Vf8BUqojeg==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    '84ed0b32-b34c-4ed2-b236-efd30a4a4022' AS product_category_adw_key,
    '49e503c9-8f79-4574-8391-68bfc6ca8f5d' AS biz_line_adw_key,
    'AG' AS product_category_cd,
    'AG' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    'HV5EdVcjrGXeFF1fihbPaw==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    'd013fba9-1a3e-47f4-91d7-475b2f9177a0' AS product_category_adw_key,
    '49e503c9-8f79-4574-8391-68bfc6ca8f5d' AS biz_line_adw_key,
    'BO' AS product_category_cd,
    'Bond' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    'kPxmShl6hRbd/KjGLbInQQ==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    '3322f59b-eea7-47cc-964e-081f0d2d695f' AS product_category_adw_key,
    '49e503c9-8f79-4574-8391-68bfc6ca8f5d' AS biz_line_adw_key,
    'CL' AS product_category_cd,
    'Commercial Lines' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    'nPjIYwHgrBk0lGFBbW9hIQ==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    '24298e7c-336a-4ce9-a89c-add7b7050c1f' AS product_category_adw_key,
    '49e503c9-8f79-4574-8391-68bfc6ca8f5d' AS biz_line_adw_key,
    'LH' AS product_category_cd,
    'Life and Health' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    'J9Ur5CHaGCLxotGu1Irz6Q==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    '8043c6a1-e864-45b8-9674-8bcfd44724d4' AS product_category_adw_key,
    '49e503c9-8f79-4574-8391-68bfc6ca8f5d' AS biz_line_adw_key,
    'OT' AS product_category_cd,
    'Other' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    'm4yyvqxjU9LYbds3nqnpjA==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number
  UNION ALL
  SELECT
    'a9b55a75-9220-4f2a-9a73-46fc952da046' AS product_category_adw_key,
    '49e503c9-8f79-4574-8391-68bfc6ca8f5d' AS biz_line_adw_key,
    'PL' AS product_category_cd,
    'Personal Lines' AS product_category_desc,
    '' AS product_category_status_cd,
    SAFE_CAST('1900-01-01' AS datetime) AS effective_start_datetime,
    SAFE_CAST('9999-12-31' AS datetime) AS effective_end_datetime,
    'Y' AS actv_ind,
    'YQwg7zOJ9EBhOCNcoW/t8g==' AS adw_row_hash,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} as integrate_insert_batch_number,
    CURRENT_DATETIME AS integrate_update_datetime,
    {{ dag_run.id }} as integrate_update_batch_number ) a
WHERE
  0 = (
  SELECT
    COUNT(product_category_adw_key)
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_product_category`);						
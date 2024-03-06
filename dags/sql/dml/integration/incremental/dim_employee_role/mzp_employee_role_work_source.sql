CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_role_work_source` AS
SELECT
  employee_role_source.agent_id,
  employee_role_source.branch_ky,
  employee_role_source.first_name,
  employee_role_source.last_name,
  employee_role_source.user_id,
  employee_role_source.agent_type_cd,
  employee_role_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY employee_role_source.agent_id ORDER BY employee_role_source.last_upd_dt DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.sales_agent` AS employee_role_source
WHERE
  CAST(employee_role_source.last_upd_dt AS datetime) > (
  SELECT
    COALESCE(MAX(effective_start_datetime),
      '1900-01-01 12:00:00')
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role`)
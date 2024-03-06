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
  ROW_NUMBER() OVER(PARTITION BY employee_role_source.agent_id, employee_role_source.last_upd_dt ORDER BY NULL DESC ) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.sales_agent` AS employee_role_source
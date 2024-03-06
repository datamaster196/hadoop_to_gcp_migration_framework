CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_role_work_transformed` AS
SELECT
  COALESCE(emp.emp_adw_key,
    "-1") AS emp_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(employee_role.first_name),''), '', coalesce(trim(employee_role.last_name),''), '', '') as nm_adw_key,
  COALESCE(office.aca_office_adw_key,
    "-1") AS aca_office_adw_key,
  'Membership' AS emp_biz_role_line_cd,
  employee_role.agent_id AS emp_role_id,
  employee_role.agent_type_cd AS emp_role_typ,
  employee_role.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT(ifnull(employee_role.agent_id,
          ''),'|',ifnull(employee_role.agent_type_cd,
          '')))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_employee_role_work_source` AS employee_role
LEFT OUTER JOIN (
  SELECT
    user_id,
    USERNAME,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY NULL ) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.cx_iusers`) users
ON
  employee_role.user_id=users.user_id
  AND users.dupe_check=1
LEFT OUTER JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` emp
ON
  emp.emp_active_directory_user_id  =users.username
  AND emp.actv_ind='Y'
LEFT OUTER JOIN (
  SELECT
    branch_ky,
    branch_cd,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_KY ORDER BY LAST_UPD_DT DESC) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.branch` ) branch
ON
  employee_role.branch_ky=branch.branch_ky
  AND branch.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    mbrs_branch_cd,
    actv_ind,
    ROW_NUMBER() OVER(PARTITION BY mbrs_branch_cd ORDER BY effective_start_datetime DESC) AS dupe_check
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` ) office
ON
  branch.branch_cd =office.mbrs_branch_cd
  AND office.dupe_check = 1
  AND office.actv_ind='Y'
WHERE
  employee_role.dupe_check=1
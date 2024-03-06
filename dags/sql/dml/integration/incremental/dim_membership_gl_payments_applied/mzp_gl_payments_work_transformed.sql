CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_mbrs_gl_payments_applied_work_transformed` AS
SELECT
  COALESCE(membership.mbrs_adw_key,
    "-1") AS mbrs_adw_key,
  COALESCE(office.aca_office_adw_key,
    COALESCE(stubbed_office.aca_office_adw_key,
      "-1")) AS aca_office_adw_key,
  office.aca_office_adw_key AS original_aca_office_adw_key,
  stubbed_office.aca_office_adw_key AS stubbed_aca_office_adw_key,
  COALESCE(payment_applied_summary.mbrs_payment_apd_summ_adw_key,
    "-1") AS mbrs_payment_apd_summ_adw_key,
  CAST(membership_gl_pay_app_source.JOURNAL_ENTRY_KY AS INT64) AS gl_payment_journal_source_key,
  gl_account.GL_DESCR AS gl_payment_gl_desc,
  gl_account.GL_ACCT_NUMBER AS gl_payment_account_nbr,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.POST_DT,1,10)) AS Date) AS gl_payment_post_dt,
  SAFE_CAST(parse_DATE('%Y-%m-%d',
      SUBSTR(membership_gl_pay_app_source.PROCESS_DT,1,10)) AS Date) AS gl_payment_process_dt,
  SAFE_CAST(membership_gl_pay_app_source.Journal_at AS NUMERIC) AS gl_payment_journal_amt,
  SAFE_CAST(membership_gl_pay_app_source.CREATE_DT AS DATETIME) AS gl_payment_journal_created_dtm,
  membership_gl_pay_app_source.DR_CR_IND AS gl_payment_journ_d_c_cd,
  membership_gl_pay_app_source.last_upd_dt AS effective_start_datetime,
  TO_BASE64(MD5(CONCAT( ifnull(COALESCE(membership.mbrs_adw_key,
            "-1"),
          ''), '|',ifnull(COALESCE(office.aca_office_adw_key,
            COALESCE(stubbed_office.aca_office_adw_key,
              "-1")),
          ''), '|',ifnull(COALESCE(payment_applied_summary.mbrs_payment_apd_summ_adw_key,
            "-1"),
          ''), '|',ifnull(gl_account.GL_DESCR,
          ''), '|',ifnull(gl_account.GL_ACCT_NUMBER,
          ''), '|',ifnull(membership_gl_pay_app_source.POST_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.PROCESS_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.Journal_at,
          ''), '|',ifnull(membership_gl_pay_app_source.CREATE_DT,
          ''), '|',ifnull(membership_gl_pay_app_source.DR_CR_IND,
          '') ))) AS adw_row_hash
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_mbrs_gl_payments_applied_work_source` AS membership_gl_pay_app_source
LEFT OUTER JOIN (
  SELECT
    mbrs_adw_key,
    mbrs_source_system_key,
    actv_ind
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership`
  WHERE
    actv_ind='Y' ) membership
ON
  CAST(membership.mbrs_source_system_key AS string)=membership_gl_pay_app_source.membership_ky
LEFT OUTER JOIN (
  SELECT
    GL_ACCOUNT_KY,
    GL_DESCR,
    GL_ACCT_NUMBER,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY GL_ACCOUNT_KY ORDER BY NULL ) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.gl_account` ) gl_account
ON
  gl_account.GL_ACCOUNT_KY=membership_gl_pay_app_source.GL_ACCOUNT_KY
  AND gl_account.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    BRANCH_CD,
    BRANCH_KY,
    ROW_NUMBER() OVER(PARTITION BY BRANCH_KY ORDER BY NULL) AS dupe_check
  FROM
    `{{ var.value.INGESTION_PROJECT }}.mzp.branch` ) branch
ON
  branch.BRANCH_KY=gl_account.BRANCH_KY
  AND branch.dupe_check=1
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    mbrs_branch_cd
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`
  WHERE
    actv_ind='Y' ) office
ON
  office.mbrs_branch_cd=branch.BRANCH_CD
LEFT OUTER JOIN (
  SELECT
    aca_office_adw_key,
    aca_office_list_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office`
  WHERE
    actv_ind='Y'
    AND aca_office_list_source_key = '-2' ) stubbed_office
ON
  stubbed_office.aca_office_list_source_key = coalesce(office.aca_office_adw_key,
    '-2')
LEFT OUTER JOIN (
  SELECT
    mbrs_payment_apd_summ_adw_key,
    payment_applied_source_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`
  WHERE
    actv_ind='Y' ) payment_applied_summary
ON
  membership_gl_pay_app_source.membership_payment_ky=CAST(payment_applied_summary.payment_applied_source_key AS string)
WHERE
  membership_gl_pay_app_source.dupe_check=1
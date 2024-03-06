
CREATE OR REPLACE TABLE
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payments_received_work_transformed` AS
SELECT
COALESCE(membership.mbrs_adw_key,'-1') mbrs_adw_key,
COALESCE(bill_summary.mbrs_billing_summary_adw_key,'-1') mbrs_billing_summary_adw_key,
COALESCE(renewal.mbr_ar_card_adw_key,'-1') mbr_ar_card_adw_key,
COALESCE(branch1.aca_office_adw_key,'-1') aca_office_adw_key,
COALESCE(users1.emp_adw_key,'-1') emp_adw_key,
payments.BATCH_KY,
payments.BATCH_PAYMENT_KY,
payments.BATCH_NAME,
payments.EXP_CT,
payments.EXP_AT,
payments.CREATE_DT,
payments.STATUS,
payments.STATUS_DT,
payments.PAYMENT_AT,
payments.PAYMENT_METHOD_CD,
CODE_PEMTH.CODE_DESC AS payment_received_method_desc,
payments.PAYMENT_SOURCE_CD,
CODE_PAYSRC.CODE_DESC AS payment_source_desc,
payments.TRANSACTION_TYPE_CD,
CODE_PAYSRC_TRANSACTION.CODE_DESC AS payment_typ_desc,
payments.REASON_CD,
payments.PAID_BY_CD,
payments.ADJUSTMENT_DESCRIPTION_CD,
CODE_PAYADJ.CODE_DESC AS payment_adj_desc,
payments.POST_COMPLETED_FL,
payments.POST_DT,
payments.AUTO_RENEW_FL,
payments.LAST_UPD_DT,
TO_BASE64(MD5(CONCAT(ifnull(COALESCE(membership.mbrs_adw_key,'-1'),''),'|',
ifnull(COALESCE(bill_summary.mbrs_billing_summary_adw_key,'-1'),''),'|',
ifnull(COALESCE(renewal.mbr_ar_card_adw_key,'-1'),''),'|',
ifnull(COALESCE(branch1.aca_office_adw_key,'-1'),''),'|',
ifnull(COALESCE(users1.emp_adw_key,'-1'),''),'|',
ifnull(payments.BATCH_PAYMENT_KY,''),'|',
ifnull(payments.BATCH_NAME,''),'|',
ifnull(payments.EXP_CT,''),'|',
ifnull(payments.EXP_AT,''),'|',
ifnull(payments.CREATE_DT,''),'|',
ifnull(payments.STATUS,''),'|',
ifnull(payments.STATUS_DT,''),'|',
ifnull(payments.PAYMENT_AT,''),'|',
ifnull(payments.PAYMENT_METHOD_CD,''),'|',
ifnull(CODE_PEMTH.CODE_DESC,''),'|',
ifnull(payments.PAYMENT_SOURCE_CD,''),'|',
ifnull(CODE_PAYSRC.CODE_DESC,''),'|',
ifnull(payments.TRANSACTION_TYPE_CD,''),'|',
ifnull(CODE_PAYSRC_TRANSACTION.CODE_DESC,''),'|',
ifnull(payments.REASON_CD,''),'|',
ifnull(payments.PAID_BY_CD,''),'|',
ifnull(payments.ADJUSTMENT_DESCRIPTION_CD,''),'|',
ifnull(CODE_PAYADJ.CODE_DESC,''),'|',
ifnull(payments.POST_COMPLETED_FL,''),'|',
ifnull(payments.POST_DT,''),'|',
ifnull(payments.AUTO_RENEW_FL,''),'|'
))) as adw_row_hash
FROM `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_payment_received_work_source` payments
LEFT JOIN
`{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` membership
ON payments.MEMBERSHIP_KY=SAFE_CAST(membership.mbrs_source_system_key AS STRING) AND membership.actv_ind='Y'
LEFT JOIN
`{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary` bill_summary
ON payments.BILL_KY=SAFE_CAST(bill_summary.bill_summary_source_key AS STRING) AND bill_summary.actv_ind='Y'
LEFT JOIN
(SELECT
branch.BRANCH_KY,
branch.BRANCH_CD,
office.mbrs_branch_cd,
office.aca_office_adw_key,
ROW_NUMBER() OVER (PARTITION BY branch.BRANCH_KY ORDER BY NULL) AS DUPE_CHECK
FROM
`{{ var.value.INGESTION_PROJECT }}.mzp.branch` branch,
`{{ var.value.INTEGRATION_PROJECT }}.adw.dim_aca_office` office
where office.mbrs_branch_cd=branch.BRANCH_CD and office.actv_ind='Y'
) as branch1
on branch1.BRANCH_KY=payments.BRANCH_KY AND branch1.DUPE_CHECK=1
LEFT JOIN
(SELECT users.USER_ID,
users.USERNAME,
emp.emp_active_directory_user_id ,
emp.emp_adw_key,
ROW_NUMBER() OVER (PARTITION BY users.USER_ID ORDER BY NULL) AS DUPE_CHECK
FROM `{{ var.value.INGESTION_PROJECT }}.mzp.cx_iusers` users,
`{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_employee` emp
where emp.emp_active_directory_user_id =users.USERNAME and emp.actv_ind='Y'
) users1 ON users1.USER_ID=payments.USER_ID AND users1.DUPE_CHECK=1
LEFT JOIN
(SELECT
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
WHERE CODE_TYPE='PAYADJ'
GROUP BY CODE) CODE_PAYADJ
ON CODE_PAYADJ.CODE=payments.ADJUSTMENT_DESCRIPTION_CD
LEFT JOIN
(SELECT
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
WHERE CODE_TYPE='PEMTH'
GROUP BY CODE) CODE_PEMTH
ON CODE_PEMTH.CODE=payments.PAYMENT_METHOD_CD
LEFT JOIN
(SELECT
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC
ON CODE_PAYSRC.CODE=payments.PAYMENT_SOURCE_CD
LEFT JOIN
(SELECT
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{{ var.value.INGESTION_PROJECT }}.mzp.cx_codes`
WHERE CODE_TYPE='PAYTYP'
GROUP BY CODE) CODE_PAYSRC_TRANSACTION
ON CODE_PAYSRC_TRANSACTION.CODE=payments.TRANSACTION_TYPE_CD
LEFT OUTER JOIN
`{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` renewal
ON SAFE_CAST(renewal.mbr_source_arc_key AS STRING)=payments.AUTORENEWAL_CARD_KY AND renewal.actv_ind='Y'
where payments.dupe_check=1
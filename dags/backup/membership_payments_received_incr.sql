CREATE OR REPLACE TABLE
  `adw-dev.adw_work.mzp_membership_payments_received_work_source` AS
  SELECT
  batch_payment.MEMBERSHIP_KY,
  batch_payment.BILL_KY,
  batch_header.BRANCH_KY,
  batch_header.USER_ID,
  batch_payment.AUTORENEWAL_CARD_KY,
  batch_payment.BATCH_KY,
  batch_payment.BATCH_PAYMENT_KY,
  batch_header.BATCH_NAME,
  batch_header.EXP_CT,
  batch_header.EXP_AT,
  batch_payment.CREATE_DT,
  batch_header.STATUS,
  batch_header.STATUS_DT,
  batch_payment.PAYMENT_AT,
  batch_payment.PAYMENT_METHOD_CD,
batch_payment.PAYMENT_METHOD_CD AS payment_received_method_description,
batch_payment.PAYMENT_SOURCE_CD,
batch_payment.PAYMENT_SOURCE_CD AS payment_source_description,
batch_payment.TRANSACTION_TYPE_CD,
batch_payment.TRANSACTION_TYPE_CD AS payment_type_description,
batch_payment.REASON_CD,
CASE WHEN batch_payment.PAID_BY_CD='P' THEN 'Payment' 
     ELSE (CASE WHEN batch_payment.PAID_BY_CD='D' THEN 'Discount' 
                ELSE (CASE WHEN batch_payment.PAID_BY_CD='V' THEN 'Voucher'
                           ELSE NULL
                           END
                      )END
          ) END AS PAID_BY_CD,
batch_payment.ADJUSTMENT_DESCRIPTION_CD,
batch_payment.POST_COMPLETED_FL,
batch_payment.POST_DT,
batch_payment.AUTO_RENEW_FL,
batch_payment.LAST_UPD_DT,
ROW_NUMBER() OVER(PARTITION BY batch_payment.BATCH_KY,batch_payment.last_upd_dt ORDER BY NULL DESC) AS dupe_check
  FROM `adw-lake-dev.mzp.batch_payment` batch_payment,
   `adw-lake-dev.mzp.batch_header` batch_header
  where batch_header.BATCH_KY=batch_payment.BATCH_KY
   AND CAST(batch_payment.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `adw-dev.member.dim_membership`)
    
------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE
    `adw-dev.adw_work.mzp_payments_received_work_transformed` AS
SELECT
membership.membership_adw_key,
bill_summary.membership_billing_summary_adw_key,
renewal.member_auto_renewal_card_adw_key,
branch1.aca_office_adw_key,
users1.employee_adw_key,
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
CODE_PEMTH.CODE_DESC AS payment_received_method_description,
payments.PAYMENT_SOURCE_CD,
CODE_PAYSRC.CODE_DESC AS payment_source_description,
payments.TRANSACTION_TYPE_CD,
CODE_PAYSRC_TRANSACTION.CODE_DESC AS payment_type_description,
payments.REASON_CD,
payments.PAID_BY_CD,
payments.ADJUSTMENT_DESCRIPTION_CD,
CODE_PAYADJ.CODE_DESC AS payment_adjustment_description,
payments.POST_COMPLETED_FL,
payments.POST_DT,
payments.AUTO_RENEW_FL,
payments.LAST_UPD_DT,
TO_BASE64(MD5(CONCAT(ifnull(membership.membership_adw_key,''),'|',
ifnull(bill_summary.membership_billing_summary_adw_key,''),'|',
ifnull(renewal.member_auto_renewal_card_adw_key,''),'|',
ifnull(branch1.aca_office_adw_key,''),'|',
ifnull(users1.employee_adw_key,''),'|',
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
FROM `adw-dev.adw_work.mzp_membership_payments_received_work_source` payments
LEFT JOIN
`adw-dev.member.dim_membership` membership
ON payments.MEMBERSHIP_KY=SAFE_CAST(membership.membership_source_system_key AS STRING) AND membership.active_indicator='Y'
LEFT JOIN
`adw-dev.member.membership_billing_summary` bill_summary
ON payments.BILL_KY=SAFE_CAST(bill_summary.bill_summary_source_key AS STRING) AND bill_summary.active_indicator='Y'
LEFT JOIN
(SELECT
branch.BRANCH_KY,
branch.BRANCH_CD,
office.membership_branch_code,
office.aca_office_adw_key
FROM
`adw-lake-dev.mzp.branch` branch,
`adw-dev.adw.dim_aca_office` office
where office.membership_branch_code=branch.BRANCH_CD and office.active_indicator='Y'
) as branch1 
on branch1.BRANCH_KY=payments.BRANCH_KY
LEFT JOIN
(SELECT users.USER_ID,
users.USERNAME,
emp.employee_active_directory_user_identifier,
emp.employee_adw_key
FROM `adw-lake-dev.mzp.cx_iusers` users,
`adw-dev.adw_pii.dim_employee` emp
where emp.employee_active_directory_user_identifier=users.USERNAME and emp.active_indicator='Y'
) users1 ON users1.USER_ID=payments.USER_ID
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`adw-lake-dev.mzp.cx_codes` 
WHERE CODE_TYPE='PAYADJ'
GROUP BY CODE) CODE_PAYADJ
ON CODE_PAYADJ.CODE=payments.ADJUSTMENT_DESCRIPTION_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`adw-lake-dev.mzp.cx_codes` 
WHERE CODE_TYPE='PEMTH'
GROUP BY CODE) CODE_PEMTH
ON CODE_PEMTH.CODE=payments.PAYMENT_METHOD_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`adw-lake-dev.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC
ON CODE_PAYSRC.CODE=payments.PAYMENT_SOURCE_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`adw-lake-dev.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC_TRANSACTION
ON CODE_PAYSRC_TRANSACTION.CODE=payments.TRANSACTION_TYPE_CD
LEFT OUTER JOIN
`adw-dev.member.dim_member_auto_renewal_card` renewal
ON SAFE_CAST(renewal.member_source_arc_key AS STRING)=payments.AUTORENEWAL_CARD_KY AND renewal.active_indicator='Y'
where payments.dupe_check=1


-----------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE
    `adw-dev.adw_work.mzp_payments_received_work_stage` AS
    SELECT
    COALESCE(target.membership_payments_received_adw_key,
      GENERATE_UUID()) membership_payments_received_adw_key,
      source.membership_adw_key membership_adw_key,
source.membership_billing_summary_adw_key membership_billing_summary_adw_key ,
source.member_auto_renewal_card_adw_key member_auto_renewal_card_adw_key ,
source.aca_office_adw_key aca_office_adw_key ,
source.employee_adw_key employee_adw_key ,
SAFE_CAST(source.BATCH_PAYMENT_KY AS INT64) payment_received_source_key ,
SAFE_CAST(source.BATCH_KY AS INT64) payment_received_batch_key ,
source.BATCH_NAME payment_received_name,
SAFE_CAST(source.EXP_CT AS INT64) payment_received_count ,
SAFE_CAST(source.EXP_AT AS INT64) payment_received_expected_amount ,
SAFE_CAST(source.CREATE_DT AS DATETIME) payment_received_created_datetime ,
source.STATUS payment_received_status ,
SAFE_CAST(source.STATUS_DT AS DATETIME) payment_received_status_datetime ,
SAFE_CAST(source.PAYMENT_AT AS INT64) payment_received_amount ,
source.PAYMENT_METHOD_CD payment_received_method_code ,
source.payment_received_method_description payment_received_method_description ,
source.PAYMENT_SOURCE_CD payment_source_code ,
source.payment_source_description payment_source_description ,
source.TRANSACTION_TYPE_CD payment_type_code ,
source.payment_type_description payment_type_description ,
source.REASON_CD payment_reason_code ,
source.PAID_BY_CD payment_paid_by_description ,
source.ADJUSTMENT_DESCRIPTION_CD payment_adjustment_code ,
source.payment_adjustment_description payment_adjustment_description ,
source.POST_COMPLETED_FL payment_received_post_flag ,
SAFE_CAST(source.POST_DT AS DATETIME) payment_received_post_datetime ,
source.AUTO_RENEW_FL payment_received_auto_renewal_flag ,
      SAFE_CAST(last_upd_dt AS DATETIME) AS effective_start_datetime,
    CAST('9999-12-31' AS datetime) effective_end_datetime,
    'Y' AS active_indicator,
    source.adw_row_hash,
    CURRENT_DATETIME() integrate_insert_datetime,
    1 integrate_insert_batch_number,
    CURRENT_DATETIME() integrate_update_datetime,
    1 integrate_update_batch_number
  FROM
    `adw-dev.adw_work.mzp_payments_received_work_transformed` source
  LEFT JOIN
    `adw-dev.member.membership_payments_received` target
  ON
    (source.BATCH_KY=SAFE_CAST(target.payment_received_batch_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    target.membership_payments_received_adw_key IS NULL
    OR source.adw_row_hash <> target.adw_row_hash
    UNION ALL
    SELECT
    target.membership_payments_received_adw_key,
target.membership_adw_key,
target.membership_billing_summary_adw_key,
target.member_auto_renewal_card_adw_key,
target.aca_office_adw_key,
target.employee_adw_key,
target.payment_received_source_key,
target.payment_received_batch_key,
target.payment_received_name,
target.payment_received_count,
target.payment_received_expected_amount,
target.payment_received_created_datetime,
target.payment_received_status,
target.payment_received_status_datetime,
target.payment_received_amount,
target.payment_received_method_code,
target.payment_received_method_description,
target.payment_source_code,
target.payment_source_description,
target.payment_type_code,
target.payment_type_description,
target.payment_reason_code,
target.payment_paid_by_description,
target.payment_adjustment_code,
target.payment_adjustment_description,
target.payment_received_post_flag,
target.payment_received_post_datetime,
target.payment_received_auto_renewal_flag,
target.effective_start_datetime,
     DATETIME_SUB(SAFE_CAST(source.last_upd_dt AS DATETIME),
      INTERVAL 1 second) AS effective_end_datetime,
    'N' AS active_indicator,
    target.adw_row_hash,
    target.integrate_insert_datetime,
    target.integrate_insert_batch_number,
    CURRENT_DATETIME(),
    1
  FROM
    `adw-dev.adw_work.mzp_payments_received_work_transformed` source
  JOIN
    `adw-dev.member.membership_payments_received` target
  ON
    (source.BATCH_KY=SAFE_CAST(target.payment_received_batch_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash
 -----------------------------------------------------------------------------------------------------------------------
    MERGE INTO
    `adw-dev.member.membership_payments_received` a
  USING
    `adw-dev.adw_work.mzp_payments_received_work_stage` b
  ON
    (a.membership_payments_received_adw_key = b.membership_payments_received_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT
    (
    membership_payments_received_adw_key,
membership_adw_key,
membership_billing_summary_adw_key,
member_auto_renewal_card_adw_key,
aca_office_adw_key,
employee_adw_key,
payment_received_source_key,
payment_received_batch_key,
payment_received_name,
payment_received_count,
payment_received_expected_amount,
payment_received_created_datetime,
payment_received_status,
payment_received_status_datetime,
payment_received_amount,
payment_received_method_code,
payment_received_method_description,
payment_source_code,
payment_source_description,
payment_type_code,
payment_type_description,
payment_reason_code,
payment_paid_by_description,
payment_adjustment_code,
payment_adjustment_description,
payment_received_post_flag,
payment_received_post_datetime,
payment_received_auto_renewal_flag,
effective_start_datetime,
effective_end_datetime,
active_indicator,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
    ) VALUES
  (  b.membership_payments_received_adw_key,
b.membership_adw_key,
b.membership_billing_summary_adw_key,
b.member_auto_renewal_card_adw_key,
b.aca_office_adw_key,
b.employee_adw_key,
b.payment_received_source_key,
b.payment_received_batch_key,
b.payment_received_name,
b.payment_received_count,
b.payment_received_expected_amount,
b.payment_received_created_datetime,
b.payment_received_status,
b.payment_received_status_datetime,
b.payment_received_amount,
b.payment_received_method_code,
b.payment_received_method_description,
b.payment_source_code,
b.payment_source_description,
b.payment_type_code,
b.payment_type_description,
b.payment_reason_code,
b.payment_paid_by_description,
b.payment_adjustment_code,
b.payment_adjustment_description,
b.payment_received_post_flag,
b.payment_received_post_datetime,
b.payment_received_auto_renewal_flag,
b.effective_start_datetime,
b.effective_end_datetime,
b.active_indicator,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number)
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.active_indicator = b.active_indicator,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number
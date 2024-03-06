from datetime import datetime, timedelta

from airflow import DAG
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u

DAG_TITLE = "integrated_mzp_membership_payments_received"

default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'start_date'      : datetime(2019, 8, 22),
    'email'           : ['airflow@example.com'],
    'catchup'         : False,
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=5),
}



######################################################################
# Load Membership_payments_received Current set of Data
######################################################################
# Queries

######################################
# Stage Work area Load - Work queries used to build staging #
######################################

######################################
# Stage Work area Load - Level 0 - Bring in source data as is
######################################


membership_payments_received_work_source = f"""
--Replace adw-dev. with {int_u.INTEGRATION_PROJECT}.
-- Replace adw-lake-dev.  with {iu.INGESTION_PROJECT}.

CREATE OR REPLACE TABLE
  `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_payments_received_work_source` AS
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
ROW_NUMBER() OVER(PARTITION BY batch_payment.BATCH_PAYMENT_KY,batch_payment.last_upd_dt ORDER BY NULL DESC) AS dupe_check
  FROM `{iu.INGESTION_PROJECT}.mzp.batch_payment` batch_payment,
   `{iu.INGESTION_PROJECT}.mzp.batch_header` batch_header
  where batch_header.BATCH_KY=batch_payment.BATCH_KY
   AND CAST(batch_payment.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{int_u.INTEGRATION_PROJECT}.member.dim_membership`)
"""
######################################
# Stage Work area Load - Level 1 - Transform and connect source data
######################################

membership_payments_received_work_transformed = f"""
CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` AS
SELECT
COALESCE(membership.membership_adw_key,'-1') membership_adw_key,
COALESCE(bill_summary.membership_billing_summary_adw_key,'-1') membership_billing_summary_adw_key,
COALESCE(renewal.member_auto_renewal_card_adw_key,'-1') member_auto_renewal_card_adw_key,
COALESCE(branch1.aca_office_adw_key,'-1') aca_office_adw_key,
COALESCE(users1.employee_adw_key,'-1') employee_adw_key,
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
TO_BASE64(MD5(CONCAT(ifnull(COALESCE(membership.membership_adw_key,'-1'),''),'|',
ifnull(COALESCE(bill_summary.membership_billing_summary_adw_key,'-1'),''),'|',
ifnull(COALESCE(renewal.member_auto_renewal_card_adw_key,'-1'),''),'|',
ifnull(COALESCE(branch1.aca_office_adw_key,'-1'),''),'|',
ifnull(COALESCE(users1.employee_adw_key,'-1'),''),'|',
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
FROM `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_membership_payments_received_work_source` payments
LEFT JOIN
`{int_u.INTEGRATION_PROJECT}.member.dim_membership` membership
ON payments.MEMBERSHIP_KY=SAFE_CAST(membership.membership_source_system_key AS STRING) AND membership.active_indicator='Y'
LEFT JOIN
`{int_u.INTEGRATION_PROJECT}.member.membership_billing_summary` bill_summary
ON payments.BILL_KY=SAFE_CAST(bill_summary.bill_summary_source_key AS STRING) AND bill_summary.active_indicator='Y'
LEFT JOIN
(SELECT
branch.BRANCH_KY,
branch.BRANCH_CD,
office.membership_branch_code,
office.aca_office_adw_key,
ROW_NUMBER() OVER (PARTITION BY branch.BRANCH_KY ORDER BY NULL) AS DUPE_CHECK
FROM
`{iu.INGESTION_PROJECT}.mzp.branch` branch,
`{int_u.INTEGRATION_PROJECT}.adw.dim_aca_office` office
where office.membership_branch_code=branch.BRANCH_CD and office.active_indicator='Y'
) as branch1 
on branch1.BRANCH_KY=payments.BRANCH_KY AND branch1.DUPE_CHECK=1
LEFT JOIN
(SELECT users.USER_ID,
users.USERNAME,
emp.employee_active_directory_user_identifier,
emp.employee_adw_key,
ROW_NUMBER() OVER (PARTITION BY users.USER_ID ORDER BY NULL) AS DUPE_CHECK
FROM `{iu.INGESTION_PROJECT}.mzp.cx_iusers` users,
`{int_u.INTEGRATION_PROJECT}.adw_pii.dim_employee` emp
where emp.employee_active_directory_user_identifier=users.USERNAME and emp.active_indicator='Y'
) users1 ON users1.USER_ID=payments.USER_ID AND users1.DUPE_CHECK=1
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYADJ'
GROUP BY CODE) CODE_PAYADJ
ON CODE_PAYADJ.CODE=payments.ADJUSTMENT_DESCRIPTION_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PEMTH'
GROUP BY CODE) CODE_PEMTH
ON CODE_PEMTH.CODE=payments.PAYMENT_METHOD_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC
ON CODE_PAYSRC.CODE=payments.PAYMENT_SOURCE_CD
LEFT JOIN
(SELECT 
CODE,
MAX(CODE_DESC) CODE_DESC
FROM
`{iu.INGESTION_PROJECT}.mzp.cx_codes` 
WHERE CODE_TYPE='PAYSRC'
GROUP BY CODE) CODE_PAYSRC_TRANSACTION
ON CODE_PAYSRC_TRANSACTION.CODE=payments.TRANSACTION_TYPE_CD
LEFT OUTER JOIN
`{int_u.INTEGRATION_PROJECT}.member.dim_member_auto_renewal_card` renewal
ON SAFE_CAST(renewal.member_source_arc_key AS STRING)=payments.AUTORENEWAL_CARD_KY AND renewal.active_indicator='Y'
where payments.dupe_check=1

"""
######################################
# Stage Work area Load - Level 2 - Establish Effective Dates
######################################

membership_payments_received_work_final_staging = f"""

CREATE OR REPLACE TABLE
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_stage` AS
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
SAFE_CAST(source.EXP_AT AS NUMERIC) payment_received_expected_amount ,
SAFE_CAST(source.CREATE_DT AS DATETIME) payment_received_created_datetime ,
source.STATUS payment_received_status ,
SAFE_CAST(source.STATUS_DT AS DATETIME) payment_received_status_datetime ,
SAFE_CAST(source.PAYMENT_AT AS NUMERIC) payment_received_amount ,
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
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` source
  LEFT JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` target
  ON
    (source.batch_payment_ky=SAFE_CAST(target.payment_received_source_key AS STRING)
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
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_transformed` source
  JOIN
    `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` target
  ON
    (source.batch_payment_ky=SAFE_CAST(target.payment_received_source_key AS STRING)
      AND target.active_indicator='Y')
  WHERE
    source.adw_row_hash <> target.adw_row_hash

	
"""

#################################
# Load Data warehouse tables from prepared data
#################################

# membership_payments_received
membership_payments_received_merge = f"""

MERGE INTO
    `{int_u.INTEGRATION_PROJECT}.member.membership_payments_received` a
  USING
    `{int_u.INTEGRATION_PROJECT}.adw_work.mzp_payments_received_work_stage` b
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
"""

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None) as dag:
    ######################################################################
    # TASKS

    # membership_payments_received

    # membership_payments_received - Workarea loads
    task_membership_payments_received_work_source = int_u.run_query("build_membership_payments_received_work_source", membership_payments_received_work_source)
    task_membership_payments_received_work_transformed = int_u.run_query("build_membership_payments_received_work_transformed", membership_payments_received_work_transformed)
    task_membership_payments_received_work_final_staging = int_u.run_query("build_membership_payments_received_work_final_staging",
                                                         membership_payments_received_work_final_staging)

    # membership_payments_received - Merge Load
    task_membership_payments_received_merge = int_u.run_query('merge_membership_payments_received', membership_payments_received_merge)


    ######################################################################
    # DEPENDENCIES



    # membership_payments_received

    task_membership_payments_received_work_source >> task_membership_payments_received_work_transformed
    task_membership_payments_received_work_transformed >> task_membership_payments_received_work_final_staging

    task_membership_payments_received_work_final_staging >> task_membership_payments_received_merge
    


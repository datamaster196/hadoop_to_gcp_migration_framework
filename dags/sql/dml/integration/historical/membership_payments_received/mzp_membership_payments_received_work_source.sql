CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_payment_received_work_source` AS
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
batch_payment.PAYMENT_METHOD_CD AS payment_received_method_desc,
batch_payment.PAYMENT_SOURCE_CD,
batch_payment.PAYMENT_SOURCE_CD AS payment_source_desc,
batch_payment.TRANSACTION_TYPE_CD,
batch_payment.TRANSACTION_TYPE_CD AS payment_typ_desc,
batch_payment.REASON_CD,
CASE WHEN batch_payment.PAID_BY_CD='P' THEN 'Payment'
     ELSE (CASE WHEN batch_payment.PAID_BY_CD='D' THEN 'Discount'
                ELSE (CASE WHEN batch_payment.PAID_BY_CD='V' THEN 'Voucher'
                           ELSE NULL
                           END
                      )END
          ) END AS PAID_BY_CD,
batch_payment.ADJUSTMENT_DESCRIPTION_CD,
case when batch_payment.POST_COMPLETED_FL is null then 'U' else batch_payment.POST_COMPLETED_FL end as POST_COMPLETED_FL,
batch_payment.POST_DT,
case when batch_payment.AUTO_RENEW_FL is null then 'U' else batch_payment.AUTO_RENEW_FL end as AUTO_RENEW_FL,
batch_payment.LAST_UPD_DT,
ROW_NUMBER() OVER(PARTITION BY batch_payment.BATCH_PAYMENT_KY,batch_payment.last_upd_dt ORDER BY NULL DESC) AS dupe_check
  FROM `{{ var.value.INGESTION_PROJECT }}.mzp.batch_payment` batch_payment,
   `{{ var.value.INGESTION_PROJECT }}.mzp.batch_header` batch_header
  where batch_header.BATCH_KY=batch_payment.BATCH_KY
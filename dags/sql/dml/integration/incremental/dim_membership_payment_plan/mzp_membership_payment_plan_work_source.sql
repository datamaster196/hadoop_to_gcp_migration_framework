CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_payment_plan_source` AS
SELECT
  PLAN_BILLING_KY,
  RIDER_KY,
  source.PAYMENT_PLAN_KY,
  PAYMENT_NUMBER,
  MEMBERSHIP_PAYMENT_KY,
  PAYMENT_STATUS,
  CHARGE_DT,
  CHARGE_CT,
  PAYMENT_AT,
  source.LAST_UPD_DT,
  COST_EFFECTIVE_DT,
  MEMBERSHIP_FEES_KY,
  DONATION_HISTORY_KY,
  MEMBERSHIP_KY,
  pp.PLAN_NAME,
  pp.NUMBER_OF_PAYMENTS,
  pp.PLAN_LENGTH,
  ROW_NUMBER() OVER(PARTITION BY source.plan_billing_ky ORDER BY source.last_upd_dt DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.plan_billing` AS source
LEFT JOIN
  `{{ var.value.INGESTION_PROJECT }}.mzp.payment_plan` AS pp
ON
  source.PAYMENT_PLAN_KY = pp.PAYMENT_PLAN_KY
WHERE
  CAST(source.last_upd_dt AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan`)
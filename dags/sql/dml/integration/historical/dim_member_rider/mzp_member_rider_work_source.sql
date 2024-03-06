CREATE OR REPLACE TABLE
`{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_rider_work_source` AS
SELECT
membership_ky,
agent_id,
rider_comp_cd,
AUTORENEWAL_CARD_KY,
member_ky,
rider.STATUS_DT  as mbr_rider_status_dtm
,rider.STATUS as mbr_rider_status_cd
,RIDER_EFFECTIVE_DT  as mbr_rider_effective_dt
,rider.CANCEL_DT  as mbr_rider_cancel_dt
,CANCEL_REASON_CD as mbr_rider_cancel_reason_cd
,rider.BILLING_CATEGORY_CD as mbr_rider_billing_category_cd
,rider.SOLICITATION_CD  as mbr_rider_solicit_cd
,COST_EFFECTIVE_DT  as mbr_rider_cost_effective_dt
,ACTUAL_CANCEL_DT  as mbr_rider_actual_cancel_dtm
,RIDER_KY  as mbr_rider_source_key
,CASE WHEN rider.DO_NOT_RENEW_FL is null then 'U'
else
rider.DO_NOT_RENEW_FL END as mbr_rider_do_not_renew_ind
,DUES_COST_AT  as mbr_rider_dues_cost_amt
,DUES_ADJUSTMENT_AT  as mbr_rider_dues_adj_amt
,PAYMENT_AT  as mbr_rider_payment_amt
,PAID_BY_CD as mbr_rider_paid_by_cd
,rider.FUTURE_CANCEL_DT  as mbr_rider_future_cancel_dt
,CASE WHEN REINSTATE_FL is null then 'U'
else REINSTATE_FL END as mbr_rider_reinstate_ind
,ADM_ORIGINAL_COST_AT  as mbr_rider_original_cost_amt
,REINSTATE_REASON_CD as mbr_rider_reinstate_reason_cd
,EXTEND_EXPIRATION_AT  as mbr_rider_extend_exp_amt
,RIDER_DEFINITION_KY  as mbr_rider_def_key
,ACTIVATION_DT  as mbr_rider_actvtn_dt
,rider.last_upd_dt  , ROW_NUMBER() OVER (PARTITION BY RIDER_KY, cast(last_upd_dt as datetime) ORDER BY NULL DESC) AS dupe_check
 FROM `{{ var.value.INGESTION_PROJECT }}.mzp.rider` rider
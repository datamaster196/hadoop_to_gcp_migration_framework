CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_membership_fee_work_source` AS
SELECT
membership_fee_source.MEMBERSHIP_FEES_KY,
membership_fee_source.membership_ky,
membership_fee_source.member_ky,
membership_fee_source.status,
membership_fee_source.fee_type,
membership_fee_source.fee_dt,
membership_fee_source.waived_dt,
membership_fee_source.waived_by,
membership_fee_source.donor_nr,
membership_fee_source.waived_reason_cd,
membership_fee_source.last_upd_dt, 
  ROW_NUMBER() OVER(PARTITION BY membership_fee_source.MEMBERSHIP_FEES_KY  ORDER BY membership_fee_source.last_upd_dt desc) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.membership_fees` AS membership_fee_source
 WHERE
   CAST(membership_fee_source.last_upd_dt AS datetime) > (
   SELECT
    MAX(effective_start_datetime)
   FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee`)
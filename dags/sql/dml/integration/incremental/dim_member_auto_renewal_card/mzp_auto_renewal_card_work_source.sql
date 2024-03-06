CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_work_source` AS
SELECT
  coalesce(TRIM(cc_first_name),
    '') AS cc_first_name,
  coalesce(TRIM(cc_last_name),
    '') AS cc_last_name,
  coalesce(TRIM(CC_STREET),
    '') AS CC_STREET,
  coalesce(TRIM(CC_City),
    '') AS CC_City,
  coalesce(TRIM(CC_State),
    '') AS CC_State,
  coalesce(TRIM(CC_Zip),
    '') AS CC_Zip,
  AUTORENEWAL_CARD_KY AS mbr_source_arc_key,
  STATUS_DT AS mbr_arc_status_dtm,
  CC_TYPE_CD AS mbr_cc_typ_cd,
  CC_EXPIRATION_DT AS mbr_cc_expiration_dt,
  CC_REJECT_REASON_CD AS mbr_cc_reject_reason_cd,
  CC_REJECT_DT AS mbr_cc_reject_dt,
  DONOR_NR AS mbr_cc_donor_nbr,
  CC_LAST_FOUR AS mbr_cc_last_four,
  CREDIT_DEBIT_TYPE AS mbr_credit_debit_typ_cd,
  CURRENT_DATETIME() AS last_upd_dt,
  adw_lake_insert_datetime,
  ROW_NUMBER() OVER (PARTITION BY AUTORENEWAL_CARD_KY ORDER BY autorenewal.STATUS_DT DESC ) AS DUP_CHECK
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.autorenewal_card` autorenewal
WHERE
  adw_lake_insert_datetime > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` )
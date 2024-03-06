CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_work_transformed` AS
SELECT
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(trim(autorenewal.cc_first_name),''),
    '',
    coalesce(trim(autorenewal.cc_last_name),''),
    '',
    '') AS nm_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(trim(autorenewal.CC_STREET),''),
    '',
    '',
    coalesce(trim(autorenewal.CC_City),''),
    coalesce(trim(autorenewal.CC_State),''),
    coalesce(trim(autorenewal.CC_Zip),''),
    '',
    '') AS address_adw_key,
  mbr_source_arc_key,
  mbr_arc_status_dtm,
  mbr_cc_typ_cd,
  mbr_cc_expiration_dt,
  mbr_cc_reject_reason_cd,
  mbr_cc_reject_dt,
  mbr_cc_donor_nbr,
  mbr_cc_last_four,
  mbr_credit_debit_typ_cd,
  CAST (autorenewal.last_upd_dt AS datetime) AS last_upd_dt,
  TO_BASE64(MD5(CONCAT( COALESCE(mbr_source_arc_key,
          ''),COALESCE(mbr_arc_status_dtm,
          ''), COALESCE(mbr_cc_typ_cd,
          ''), COALESCE(mbr_cc_expiration_dt,
          ''), COALESCE(mbr_cc_reject_reason_cd,
          ''), COALESCE(mbr_cc_reject_dt,
          ''), COALESCE(mbr_cc_donor_nbr,
          ''), COALESCE(mbr_cc_last_four,
          ''), COALESCE(mbr_credit_debit_typ_cd,
          '') ))) AS adw_row_hash,
  ROW_NUMBER() OVER (PARTITION BY mbr_source_arc_key ORDER BY last_upd_dt DESC ) AS DUP_CHECK
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_work_source` autorenewal
WHERE
  autorenewal.dup_check=1
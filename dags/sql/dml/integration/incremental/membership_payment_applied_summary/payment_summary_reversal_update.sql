UPDATE
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary` a
SET
  a.mbrs_payment_apd_rev_adw_key=coalesce(b.mbrs_payment_apd_summ_adw_key,
    a.mbrs_payment_apd_rev_adw_key)
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary` b
WHERE
  SAFE_CAST(a.mbrs_payment_apd_rev_adw_key AS int64)=b.payment_applied_source_key
  AND SAFE_CAST(a.mbrs_payment_apd_rev_adw_key AS int64) IS NOT NULL
  AND a.mbrs_payment_apd_rev_adw_key != '-1'
  AND b.actv_ind='Y' /*; 

   --1. -- Orphaned foreign key check for mbrs_payment_apd_rev_adw_key
  SELECT
  COUNT(target. mbrs_payment_apd_rev_adw_key ) AS mbrs_payment_apd_rev_adw_key_count
FROM (
  SELECT
    DISTINCT mbrs_payment_apd_rev_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) target
WHERE
  NOT EXISTS (
  SELECT
    1
  FROM (
    SELECT
      DISTINCT mbrs_payment_apd_summ_adw_key
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary`) source_FK_1
  WHERE
    target.mbrs_payment_apd_rev_adw_key = source_FK_1.mbrs_payment_apd_summ_adw_key)
HAVING
IF
  ((mbrs_payment_apd_rev_adw_key_count = 0 ),
    TRUE,
    ERROR('Error: FK check failed for adw.mbrs_payment_applied_summary. FK Column: mbrs_payment_apd_rev_adw_key'));
    */
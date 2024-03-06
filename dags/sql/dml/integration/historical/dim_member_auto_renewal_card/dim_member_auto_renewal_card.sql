INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` (mbr_ar_card_adw_key,
    nm_adw_key,
    address_adw_key,
    mbr_source_arc_key,
    mbr_arc_status_dtm,
    mbr_cc_typ_cd,
    mbr_cc_expiration_dt,
    mbr_cc_reject_reason_cd,
    mbr_cc_reject_dt,
    mbr_cc_donor_nbr,
    mbr_cc_last_four,
    mbr_credit_debit_typ_cd,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number)
SELECT
  SAFE_CAST(mbr_ar_card_adw_key AS STRING),
  SAFE_CAST(nm_adw_key AS STRING),
  SAFE_CAST(address_adw_key AS STRING),
  SAFE_CAST(hist_type_2.mbr_source_arc_key AS INT64),
  SAFE_CAST(mbr_arc_status_dtm AS DATETIME),
  mbr_cc_typ_cd,
  SAFE_CAST(TIMESTAMP(mbr_cc_expiration_dt) AS DATE),
  mbr_cc_reject_reason_cd,
  SAFE_CAST(TIMESTAMP(mbr_cc_reject_dt) AS DATE),
  mbr_cc_donor_nbr,
  mbr_cc_last_four,
  mbr_credit_debit_typ_cd,
  CAST(hist_type_2.effective_start_datetime AS datetime) AS effective_start_datetime,
  CAST(hist_type_2.effective_end_datetime AS datetime) AS effective_end_datetime,
  CASE
    WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
  ELSE
  'N'
END
  AS actv_ind,
  source.adw_row_hash,
  CURRENT_DATETIME(),
  {{ dag_run.id }},
  CURRENT_DATETIME(),
  {{ dag_run.id }}
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_Work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_work_transformed` source
ON
  ( source.mbr_source_arc_key=hist_type_2.mbr_source_arc_key
    AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    GENERATE_UUID() AS mbr_ar_card_adw_key,
    mbr_source_arc_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_auto_renewal_card_work_transformed`
  GROUP BY
    mbr_source_arc_key) pk
ON
  (source.mbr_source_arc_key=pk.mbr_source_arc_key);



--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- Orphaned foreign key check for dim_member_auto_renewal_card


-- nm_adw_key
SELECT
        count(target. nm_adw_key ) AS nm_adw_key
FROM
        (select distinct nm_adw_key
        from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card`)  target
          where not exists (select 1
                         from (select distinct nm_adw_key
                               from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name`) source_FK_1
                               where target.nm_adw_key = source_FK_1.nm_adw_key)
HAVING
IF((nm_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_member_auto_renewal_card. FK Column: nm_adw_key'));

-- address_adw_key

SELECT
        count(target. address_adw_key ) AS address_adw_key
FROM
        (select distinct address_adw_key
        from  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card`)  target
          where not exists (select 1
                         from (select distinct address_adw_key
                               from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address`) source_FK_1
                               where target.address_adw_key = source_FK_1.address_adw_key)
HAVING
IF((address_adw_key = 0  ), true, ERROR('Error: FK check failed for adw.dim_member_auto_renewal_card. FK Column: address_adw_key'));



--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
    (select mbr_source_arc_key, effective_start_datetime, count(*) as dupe_count
     from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card`
     group by 1, 2
     having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_member_auto_renewal_card'  ) );


---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a. mbr_ar_card_adw_key )
from
  (select mbr_ar_card_adw_key , effective_start_datetime, effective_end_datetime from
   `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card`) a
join
  (select mbr_ar_card_adw_key, effective_start_datetime, effective_end_datetime from
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card`) b
on a.mbr_ar_card_adw_key=b.mbr_ar_card_adw_key
       and a.effective_start_datetime  <= b.effective_end_datetime
       and b.effective_start_datetime  <= a.effective_end_datetime
       and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.mbr_ar_card_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_member_auto_renewal_card' ));

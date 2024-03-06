INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` ( mbrs_solicit_adw_key,
    mbrs_solicit_source_system_key,
    solicit_cd,
    solicit_group_cd,
    mbr_typ_cd,
    solicit_camp_cd,
    solicit_category_cd,
    solicit_discount_cd,
    effective_start_datetime,
    effective_end_datetime,
    actv_ind,
    adw_row_hash,
    integrate_insert_datetime,
    integrate_insert_batch_number,
    integrate_update_datetime,
    integrate_update_batch_number ) (
  SELECT
    mbrs_solicit_adw_key,
    SAFE_CAST(source.solicitation_ky AS INT64),
    source.solicitation_cd,
    source.group_cd,
    source.membership_type_cd,
    source.campaign_cd,
    source.solicitation_category_cd,
    source.discount_cd,
    CAST(hist_type_2.effective_start_datetime AS datetime),
    CAST(hist_type_2.effective_end_datetime AS datetime),
    (
      CASE
        WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y'
      ELSE
      'N'
    END
      ),
    source.adw_row_hash,
    CURRENT_DATETIME(),
    {{ dag_run.id }},
    CURRENT_DATETIME(),
    {{ dag_run.id }}
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_type_2_hist` hist_type_2
  JOIN
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_transformed` source
  ON
    ( source.solicitation_ky=hist_type_2.solicitation_ky
      AND source.last_upd_dt=hist_type_2.effective_start_datetime)
  LEFT JOIN (
    SELECT
      GENERATE_UUID() AS mbrs_solicit_adw_key,
      solicitation_ky
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_solicitation_work_transformed`
    GROUP BY
      solicitation_ky) pk
  ON
    (source.solicitation_ky=pk.solicitation_ky) );

--------------------------------Audit Validation Queries---------------------------------------
-----------------------------------------------------------------------------------------------
-- No Orphaned foreign key check for dim_membership_solicitation

--------------------------------------------------------------------------------------------
-- Duplicate Checks

select count(1)
from
    (select mbrs_solicit_source_system_key , solicit_cd, effective_start_datetime, count(*) as dupe_count
     from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation`
     group by 1, 2, 3
     having count(*)>1 ) x
HAVING
IF (count(1) = 0, true, ERROR( 'Error: Duplicate Records check failed for adw.dim_membership_solicitation'  ) );


---------------------------------------------------------------------------------------------
-- Effective Dates overlapping check

select count(a.mbrs_solicit_adw_key ) from
   (select mbrs_solicit_adw_key , mbrs_solicit_source_system_key, effective_start_datetime, effective_end_datetime from
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation`) a
join
   (select mbrs_solicit_adw_key, mbrs_solicit_source_system_key, effective_start_datetime, effective_end_datetime from
   `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation`) b
on a.mbrs_solicit_adw_key=b.mbrs_solicit_adw_key
        and a.mbrs_solicit_source_system_key = b.mbrs_solicit_source_system_key
        and a.effective_start_datetime  <= b.effective_end_datetime
        and b.effective_start_datetime  <= a.effective_end_datetime
        and a.effective_start_datetime  <> b.effective_start_datetime
HAVING IF ((count(a.mbrs_solicit_adw_key) = 0), true, ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_membership_solicitation' ));
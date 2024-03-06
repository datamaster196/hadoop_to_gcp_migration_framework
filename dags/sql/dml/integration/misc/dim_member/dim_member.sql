INSERT INTO
  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`
  (
  mbr_adw_key,
mbrs_adw_key,
contact_adw_key,
mbrs_solicit_adw_key,
emp_role_adw_key,
mbr_source_system_key,
mbrs_id ,
mbr_assoc_id ,
mbr_ck_dgt_nbr,
mbr_status_cd,
mbr_extended_status_cd,
mbr_expiration_dt,
mbr_card_expiration_dt,
prim_mbr_cd,
mbr_do_not_renew_ind,
mbr_billing_cd,
mbr_billing_category_cd,
mbr_status_dtm,
mbr_cancel_dt,
mbr_join_aaa_dt,
mbr_join_club_dt,
mbr_reason_joined_cd,
mbr_assoc_relation_cd,
mbr_solicit_cd,
mbr_source_of_sale_cd,
mbr_comm_cd,
mbr_future_cancel_ind,
mbr_future_cancel_dt,
mbr_renew_method_cd,
free_mbr_ind,
previous_club_mbrs_id ,
previous_club_cd,
mbr_merged_previous_key,
mbr_merged_previous_club_cd,
mbr_mbrs_merged_previous_id ,
mbr_assoc_merged_previous_id,
mbr_merged_previous_ck_dgt_nbr,
mbr_merged_previous_mbr16_id ,
mbr_change_nm_dtm,
mbr_email_changed_dtm,
mbr_bad_email_ind,
--mbr_email_optout_ind,
mbr_web_last_login_dtm,
mbr_active_expiration_dt,
mbr_actvtn_dt,
--mbr_dn_text_ind,
mbr_duplicate_email_ind,
--mbr_dn_call_ind,
--mbr_no_email_ind,
--mbr_dn_mail_ind,
mbr_dn_ask_for_email_ind,
--mbr_dn_email_ind,
mbr_refused_give_email_ind,
mbrs_purge_ind,
payment_plan_future_cancel_dt,
payment_plan_future_cancel_ind,
mbr_card_typ_cd,
mbr_card_typ_desc,
entitlement_start_dt,
entitlement_end_dt,
mbr_previous_expiration_dt,
effective_start_datetime,
effective_end_datetime,
actv_ind,
adw_row_hash,
integrate_insert_datetime,
integrate_insert_batch_number,
integrate_update_datetime,
integrate_update_batch_number
  )
SELECT
mbr_adw_key,
COALESCE(source.mbrs_adw_key,'-1') ,
COALESCE(source.contact_adw_key,'-1') ,
COALESCE(mbrs_solicit_adw_key,'-1'),
COALESCE(source.emp_role_adw_key,'-1') ,
SAFE_CAST(source.mbr_source_system_key AS INT64) ,
source.mbrs_id  ,
source.mbr_assoc_id  ,
SAFE_CAST(source.mbr_ck_dgt_nbr AS INT64) ,
source.mbr_status_cd ,
source.mbr_extended_status_cd ,
CAST(substr(source.mbr_expiration_dt,0,10) AS date) ,
CAST(substr(source.member_card_expiration_dt,0,10) AS date)  ,
source.prim_mbr_cd ,
source.mbr_do_not_renew_ind ,
source.mbr_billing_cd ,
source.mbr_billing_category_cd ,
CAST(source.mbr_status_dtm as datetime) ,
CAST(substr(source.mbr_cancel_dt,0,10) AS date) ,
CASE WHEN SUBSTR(source.mbr_join_aaa_dt,1,3) IN ('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec')
    THEN parse_date('%h %d %Y',substr(source.mbr_join_aaa_dt,0,11))
    ELSE parse_date('%d-%b-%y',substr(source.mbr_join_aaa_dt,0,10))
    END
,
CAST(substr(source.mbr_join_club_dt,0,10) AS date) ,
source.mbr_reason_joined_cd ,
source.mbr_assoc_relation_cd ,
source.mbr_solicit_cd ,
source.mbr_source_of_sale_cd ,
source.commission_cd ,
source.mbr_future_cancel_ind ,
CAST(substr(source.mbr_future_cancel_dt,0,10) AS date) ,
source.mbr_renew_method_cd ,
source.free_mbr_ind ,
source.previous_club_mbrs_id  ,
source.member_previous_club_cd ,
safe_cast(source.mbr_merged_previous_key as int64) ,
source.mbr_merged_previous_club_cd ,
source.mbr_mbrs_merged_previous_id  ,
source.mbr_assoc_merged_previous_id ,
source.mbr_merged_previous_ck_dgt_nbr ,
source.mbr_merged_previous_mbr16_id  ,
SAFE_CAST(source.mbr_change_nm_dtm AS DATETIME) ,
SAFE_CAST(source.mbr_email_changed_dtm AS DATETIME) ,
source.mbr_bad_email_ind ,
--source.mbr_email_optout_ind ,
SAFE_CAST(source.mbr_web_last_login_dtm AS DATETIME) ,
CAST(substr (source.mbr_active_expiration_dt,0,10) AS date) ,
CAST(substr (source.mbr_actvtn_dt,0,10) AS date) ,
--SAFE_CAST(source.mbr_dn_text_ind AS STRING) ,
SAFE_CAST(source.mbr_duplicate_email_ind AS STRING) ,
--SAFE_CAST(source.mbr_dn_call_ind AS STRING) ,
--SAFE_CAST(source.mbr_no_email_ind AS STRING) ,
--SAFE_CAST(source.mbr_dn_mail_ind AS STRING) ,
SAFE_CAST(source.mbr_dn_ask_for_email_ind AS STRING) ,
--SAFE_CAST(source.mbr_dn_email_ind AS STRING) ,
SAFE_CAST(source.mbr_refused_give_email_ind AS STRING) ,
SAFE_CAST(source.mbrs_purge_ind AS STRING) ,
CAST(substr(source.payment_plan_future_cancel_dt, 0, 10) AS date) ,
source.payment_plan_future_cancel_ind ,
source.mbr_card_typ_cd ,
source.mbr_card_typ_desc,
CAST(substr(source.entitlement_start_dt, 0, 10) AS date) ,
CAST(substr(source.entitlement_end_dt, 0, 10) AS date) ,
CAST(substr(source.mbr_previous_expiration_dt, 0, 10) AS date) ,
CAST(hist_type_2.effective_start_datetime AS datetime),
CAST(hist_type_2.effective_end_datetime AS datetime),
(CASE WHEN hist_type_2.effective_end_datetime=datetime('9999-12-31') THEN 'Y' ELSE 'N' END) ,
source.adw_row_hash,
CURRENT_DATETIME() ,
{{ dag_run.id }},
CURRENT_DATETIME() ,
{{ dag_run.id }}
    FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_type_2_hist` hist_type_2
JOIN
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_transformed` source
  ON (
  source.mbr_source_system_key=hist_type_2.mbr_source_system_key
  AND source.last_upd_dt=hist_type_2.effective_start_datetime)
LEFT JOIN (
  SELECT
    mbr_adw_key,
    mbr_source_system_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_member_backup`
  GROUP BY
    mbr_adw_key, mbr_source_system_key) pk
ON (safe_cast(source.mbr_source_system_key as int64)=pk.mbr_source_system_key);

UPDATE  `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` m
SET mbr_extended_status_cd = CASE
                             WHEN m.mbr_status_cd = 'C' or ms.mbrs_status_cd = 'C' THEN 'C'
                             WHEN ms.mbrs_status_cd = 'P' and ms.mbrs_salvage_ind = 'Y' THEN 'S'
                             WHEN ms.mbrs_status_cd <> 'C' and ifnull(ms.mbrs_salvage_ind,'N') = 'N' and m.mbr_expiration_dt < date_Add( CURRENT_DATE(),INTERVAL 60 DAY) THEN 'G'
                             WHEN ms.mbrs_status_cd <> 'C' and m.mbr_expiration_dt >= CURRENT_DATE() THEN  m.mbr_status_cd
                             ELSE 'C' END
FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` ms
WHERE m.mbrs_adw_key = ms.mbrs_adw_key
and m.actv_ind = 'Y'
and ms.actv_ind = 'Y';
   --------------------------------Audit Validation Queries---------------------------------------
  -----------------------------------------------------------------------------------------------
  -- Orphaned foreign key check for mbrs_adw_key

SELECT
     count(target.mbrs_adw_key) AS mbrs_adw_key_count
 FROM
     (select distinct mbrs_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`)  target
       where not exists (select 1
                      from (select distinct mbrs_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership`) source_FK_1
                          where target.mbrs_adw_key = source_FK_1.mbrs_adw_key)
HAVING
 IF((mbrs_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member. FK Column: mbrs_adw_key'));
 
  -- Orphaned foreign key check for contact_adw_key
 SELECT
     count(target.contact_adw_key ) AS contact_adw_key_count
 FROM
     (select distinct contact_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`)  target
       where not exists (select 1
                      from (select distinct contact_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw_pii.dim_contact_info`) source_FK_2
                          where target.contact_adw_key = source_FK_2.contact_adw_key)
HAVING
 IF((contact_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member. FK Column: contact_adw_key '));

  -- Orphaned foreign key check for mbrs_solicit_adw_key
 SELECT
     count(target.mbrs_solicit_adw_key ) AS mbrs_solicit_adw_key_count
 FROM
     (select distinct mbrs_solicit_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`where  mbrs_solicit_adw_key<>'-1' )  target
       where not exists (select 1
                      from (select distinct mbrs_solicit_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_membership_solicitation` where  mbrs_solicit_adw_key<>'-1'  ) source_FK_3
                          where target.mbrs_solicit_adw_key = source_FK_3.mbrs_solicit_adw_key)
HAVING
 IF((mbrs_solicit_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member. FK Column: mbrs_solicit_adw_key'));

  -- Orphaned foreign key check for emp_role_adw_key
 SELECT
     count(target.emp_role_adw_key ) AS emp_role_adw_key_count
 FROM
     (select distinct emp_role_adw_key
   from  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`)  target
       where not exists (select 1
                      from (select distinct emp_role_adw_key
          from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_employee_role`) source_FK_4
                          where target.emp_role_adw_key = source_FK_4.emp_role_adw_key)
HAVING
 IF((emp_role_adw_key_count = 0  ), true, ERROR('Error: FK check failed for adw.dim_member. FK Column: emp_role_adw_key'));
 
  ----------------------------------------------------------------------------------------------
  -- Duplicate Checks
SELECT
  COUNT(1)
FROM (
  SELECT
    mbr_source_system_key,
    effective_start_datetime,
    COUNT(*) AS dupe_count
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`
  GROUP BY
    1,
    2
  HAVING
    COUNT(*)>1 ) x
HAVING
IF
  (COUNT(1) = 0,
    TRUE,
    ERROR( 'Error: Duplicate Records check failed for adw.dim_member' ));
  ---------------------------------------------------------------------------------------------
  -- Effective Dates overlapping check
SELECT
  COUNT(a.mbr_adw_key)
FROM (
  SELECT
    mbr_adw_key,
    mbr_source_system_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`) a
JOIN (
  SELECT
    mbr_adw_key,
    mbr_source_system_key,
    effective_start_datetime,
    effective_end_datetime
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_member`) b
ON
  a.mbr_adw_key=b.mbr_adw_key
  AND a.mbr_source_system_key = b.mbr_source_system_key
  AND a.effective_start_datetime <= b.effective_end_datetime
  AND b.effective_start_datetime <= a.effective_end_datetime
  AND a.effective_start_datetime <> b.effective_start_datetime
HAVING
IF
  ((COUNT(a.mbr_adw_key) = 0),
    TRUE,
    ERROR( 'Error: Effective Dates Overlap check failed for adw.dim_member' ));

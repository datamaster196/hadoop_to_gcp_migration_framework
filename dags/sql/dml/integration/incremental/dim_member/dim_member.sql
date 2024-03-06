MERGE INTO
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` a
  USING
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_stage` b
    ON (a.mbr_adw_key = b.mbr_adw_key
    AND a.effective_start_datetime = b.effective_start_datetime)
    WHEN NOT MATCHED THEN INSERT(
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
    ) VALUES
    (
    b.mbr_adw_key,
b.mbrs_adw_key,
b.contact_adw_key,
b.mbrs_solicit_adw_key,
b.emp_role_adw_key,
b.mbr_source_system_key,
b.mbrs_id ,
b.mbr_assoc_id ,
b.mbr_ck_dgt_nbr,
b.mbr_status_cd,
b.mbr_extended_status_cd,
b.mbr_expiration_dt,
b.member_card_expiration_dt,
b.prim_mbr_cd,
b.mbr_do_not_renew_ind,
b.mbr_billing_cd,
b.mbr_billing_category_cd,
b.mbr_status_dtm,
b.mbr_cancel_dt,
b.mbr_join_aaa_dt,
b.mbr_join_club_dt,
b.mbr_reason_joined_cd,
b.mbr_assoc_relation_cd,
b.mbr_solicit_cd,
b.mbr_source_of_sale_cd,
b.commission_cd,
b.mbr_future_cancel_ind,
b.mbr_future_cancel_dt,
b.mbr_renew_method_cd,
b.free_mbr_ind,
b.previous_club_mbrs_id ,
b.member_previous_club_cd,
b.mbr_merged_previous_key,
b.mbr_merged_previous_club_cd,
b.mbr_mbrs_merged_previous_id ,
b.mbr_assoc_merged_previous_id,
b.mbr_merged_previous_ck_dgt_nbr,
b.mbr_merged_previous_mbr16_id ,
b.mbr_change_nm_dtm,
b.mbr_email_changed_dtm,
b.mbr_bad_email_ind,
--b.mbr_email_optout_ind,
b.mbr_web_last_login_dtm,
b.mbr_active_expiration_dt,
b.mbr_actvtn_dt,
--b.mbr_dn_text_ind,
b.mbr_duplicate_email_ind,
--b.mbr_dn_call_ind,
--b.mbr_no_email_ind,
--b.mbr_dn_mail_ind,
b.mbr_dn_ask_for_email_ind,
--b.mbr_dn_email_ind,
b.mbr_refused_give_email_ind,
b.mbrs_purge_ind,
b.payment_plan_future_cancel_dt,
b.payment_plan_future_cancel_ind,
b.mbr_card_typ_cd,
b.mbr_card_typ_desc,
b.entitlement_start_dt,
b.entitlement_end_dt,
b.mbr_previous_expiration_dt,
b.effective_start_datetime,
b.effective_end_datetime,
b.actv_ind,
b.adw_row_hash,
b.integrate_insert_datetime,
b.integrate_insert_batch_number,
b.integrate_update_datetime,
b.integrate_update_batch_number
    )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.effective_end_datetime = b.effective_end_datetime,
    a.actv_ind = b.actv_ind,
    a.integrate_update_datetime = b.integrate_update_datetime,
    a.integrate_update_batch_number = b.integrate_update_batch_number;

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

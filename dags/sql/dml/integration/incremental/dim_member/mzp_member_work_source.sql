CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_work_source` AS
 SELECT
 member_source.MEMBER_KY ,
 member_source.MEMBERSHIP_KY,
member_source.STATUS ,
member_source.CHECK_DIGIT_NR ,
member_source.ASSOCIATE_ID ,
member_source.MEMBERSHIP_ID ,
member_source.MEMBER_TYPE_CD ,
member_source.RENEW_METHOD_CD ,
member_source.COMMISSION_CD ,
member_source.SOURCE_OF_SALE ,
member_source.SOLICITATION_CD ,
member_source.ASSOCIATE_RELATION_CD,
member_source.JOIN_CLUB_DT,
member_source.JOIN_AAA_DT,
member_source.CANCEL_DT,
member_source.STATUS_DT,
member_source.BILLING_CATEGORY_CD,
member_source.BILLING_CD,
'N' as mbrs_purge_ind,
member_source.MEMBER_EXPIRATION_DT,
member_source.MEMBER_CARD_EXPIRATION_DT,
member_source.REASON_JOINED,
CASE WHEN member_source.DO_NOT_RENEW_FL is null then 'U'
else
member_source.DO_NOT_RENEW_FL END as DO_NOT_RENEW_FL,
member_source.FUTURE_CANCEL_DT,
CASE WHEN member_source.FUTURE_CANCEL_FL is null then 'U'
else
member_source.FUTURE_CANCEL_FL END as FUTURE_CANCEL_FL,
CASE WHEN member_source.FREE_MEMBER_FL is null then 'U'
else
member_source.FREE_MEMBER_FL END as FREE_MEMBER_FL,
member_source.PREVIOUS_MEMBERSHIP_ID,
member_source.PREVIOUS_CLUB_CD,
member_source.NAME_CHANGE_DT,
member_source.ADM_EMAIL_CHANGED_DT,
CASE WHEN member_source.BAD_EMAIL_FL is null then 'U'
else
member_source.BAD_EMAIL_FL END as BAD_EMAIL_FL,
member_source.EMAIL_OPTOUT_FL,
member_source.WEB_LAST_LOGIN_DT,
member_source.ACTIVE_EXPIRATION_DT,
member_source.ACTIVATION_DT,
member_source.AGENT_ID,
member_source.LAST_UPD_DT,
member_source.pp_fc_fl,
member_source.pp_fc_dt,
member_source.credential_ky,
member_source.entitlement_start_dt,
member_source.entitlement_end_dt,
member_source.previous_expiration_dt,
ROW_NUMBER() OVER(PARTITION BY member_source.MEMBER_KY ORDER BY  member_source.LAST_UPD_DT DESC) AS DUPE_CHECK
  FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.member` AS member_source
WHERE
  CAST(member_source.LAST_UPD_DT AS datetime) > (
  SELECT
    MAX(effective_start_datetime)
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`)
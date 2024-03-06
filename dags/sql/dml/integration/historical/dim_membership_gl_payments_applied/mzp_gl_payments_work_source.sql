CREATE OR REPLACE TABLE
   `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_mbrs_gl_payments_applied_work_source` AS
 SELECT
 membership_gl_pay_app_source.JOURNAL_ENTRY_KY ,
 membership_gl_pay_app_source.GL_ACCOUNT_KY ,
 membership_gl_pay_app_source.membership_ky,
 membership_gl_pay_app_source.membership_payment_ky,
 membership_gl_pay_app_source.POST_DT ,
 membership_gl_pay_app_source.PROCESS_DT ,
 membership_gl_pay_app_source.Journal_at ,
 membership_gl_pay_app_source.CREATE_DT ,
 membership_gl_pay_app_source.DR_CR_IND ,
 membership_gl_pay_app_source.last_upd_dt,

   ROW_NUMBER() OVER(PARTITION BY  membership_gl_pay_app_source.JOURNAL_ENTRY_KY, membership_gl_pay_app_source.last_upd_dt  ORDER BY  null ) AS dupe_check
 FROM
   `{{ var.value.INGESTION_PROJECT }}.mzp.journal_entry` AS membership_gl_pay_app_source

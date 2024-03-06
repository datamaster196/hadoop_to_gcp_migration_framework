INSERT INTO `{{ var.value.INGESTION_PROJECT }}.admin.adw_record_count_validation`
  (data_layer, source_system, table_name, count_type, record_count, adw_insert_datetime, adw_insert_batch_number)
select 'source', 'mzp', table_name, 'table_count',  record_count, adw_lake_insert_datetime, adw_lake_insert_batch_number
from `{{ var.value.INGESTION_PROJECT }}.work.stg_mzp_record_count_validation`
where owner = 'MZ_PLUS_212'
  and table_name
  in ('CX_CODES','CX_CODE_TYPE','CX_IUSERS',
  'MZ_AUTORENEWAL_CARD', 'MZ_BATCH_HEADER','MZ_BILL_DETAIL', 'MZ_BILL_SUMMARY', 'MZ_BRANCH', 'MZ_CLUB',
  'MZ_DAILY_COUNTS_SUMMARY', 'MZ_DISCOUNT_HISTORY', 'MZ_DIVISION', 'MZ_GL_ACCOUNT', 'MZ_JOURNAL_ENTRY',
  'MZ_MEMBER', 'MZ_MEMBERSHIP_CODE', 'MZ_MEMBERSHIP_CROSSREF', 'MZ_MEMBER_CODE', 'MZ_MEMBER_CROSSREF',
  'MZ_MEMBERSHIP_FEES', 'MZ_PAYMENT_DETAIL', 'MZ_PAYMENT_PLAN', 'MZ_PAYMENT_SUMMARY', 'MZ_RENEWAL_METHOD_AUDIT',
  'MZ_RETENTION', 'MZ_RETENTION_OPTION', 'MZ_RIDER', 'MZ_SALES_AGENT', 'MZ_SALES_AGENT_ACTIVITY',
  'MZ_SEGMENTATION_HISTORY', 'MZ_SEGMENTATION_SETUP', 'MZ_SOLICITATION', 'MZ_SOLICITATION_DISCOUNT', 'MZ_TERRITORY',
  'MZ_MEMBERSHIP', 'MZ_BATCH_PAYMENT', 'MZ_MEMBERSHIP_COMMENT')
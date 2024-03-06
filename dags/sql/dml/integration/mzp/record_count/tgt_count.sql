INSERT INTO `{{ var.value.INGESTION_PROJECT }}.admin.adw_record_count_validation`
  (data_layer, source_system, table_name, count_type, record_count, adw_insert_datetime, adw_insert_batch_number)

select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_comment', 'distinct_table_count', count(distinct comment_adw_key), CURRENT_DATETIME, {{ dag_run.id }}  from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_comment` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_member', 'distinct_table_count', count(distinct mbr_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_member_auto_renewal_card', 'distinct_table_count', count(distinct mbr_ar_card_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_auto_renewal_card` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_member_rider', 'distinct_table_count', count(distinct mbr_rider_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member_rider` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_membership', 'distinct_table_count', count(distinct mbrs_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_membership_fee', 'distinct_table_count', count(distinct mbrs_fee_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_fee` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_membership_payment_plan', 'distinct_table_count', count(distinct mbrs_payment_plan_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_payment_plan` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_membership_solicitation', 'distinct_table_count', count(distinct mbrs_solicit_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_membership_solicitation` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_product', 'distinct_table_count', count(distinct product_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'dim_product_category', 'distinct_table_count', count(distinct product_category_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_product_category` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'mbrs_gl_payments_applied', 'distinct_table_count', count(distinct mbrs_gl_pymt_applied_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_gl_payments_applied` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'mbrs_payment_applied_detail', 'distinct_table_count', count(distinct mbrs_payment_apd_dtl_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_detail` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'mbrs_payment_applied_summary', 'distinct_table_count', count(distinct mbrs_payment_apd_summ_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.mbrs_payment_applied_summary` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'membership_billing_detail', 'distinct_table_count', count(distinct mbrs_billing_detail_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_detail` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'membership_billing_summary', 'distinct_table_count', count(distinct mbrs_billing_summary_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_billing_summary` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'membership_payment_received', 'distinct_table_count', count(distinct mbrs_payment_received_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.membership_payment_received` union all
select 'integration', '{{ var.value.INTEGRATION_PROJECT }}', 'sales_agent_activity', 'distinct_table_count', count(distinct saa_adw_key ), CURRENT_DATETIME, {{ dag_run.id }} from `{{ var.value.INTEGRATION_PROJECT }}.adw.sales_agent_activity`
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.d3.arch_call_extd`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.d3.arch_call`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.d3.call_adj`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.d3.caller`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.d3.service_facility`;

Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.contactnumber`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.policy`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.cdlinestatus`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.profitcenter`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.client`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.employee`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.contactname`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.cdpolicylinetype`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.contactaddress`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.line`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.entityemployeejt`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.company`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.epic.securityuser`;

Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.cx_iusers`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.membership_comment`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.payment_plan`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.territory`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.membership_crossref`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.division`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.cx_code_type`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.autorenewal_card`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.segmentation_setup`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.solicitation_discount`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.bill_summary`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.cx_codes`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.daily_counts_summary`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.retention`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.member`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.bill_detail`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.gl_account`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.batch_header`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.sales_agent`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.membership_code`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.rider`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.plan_billing`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.branch`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.payment_detail`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.membership`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.bill_info`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.payment_summary`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.sales_agent_activity`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.batch_payment`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.member_crossref`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.renewal_method_audit`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.aca_store_list`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.journal_entry`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.discount_history`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.club`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.membership_fees`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.solicitation`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.member_code`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.retention_option`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.segmentation_history`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.membership_balance`;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mzp.credential`;

Drop 	table 	if 	exists 	`{{params.ingestion_project}}.mba.mstr_customer`;

Drop 	table 	if 	exists 	`{{params.ingestion_project}}`.sfmc.open;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}`.sfmc.bounce;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}`.sfmc.click;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}`.sfmc.listsubscribers;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}`.sfmc.subscribers;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}`.sfmc.unsubscribe;
Drop 	table 	if 	exists 	`{{params.ingestion_project}}`.sfmc.sendlog;

Delete from `{{params.ingestion_project}}.admin.ingestion_highwatermarks` where 1=1;
insert into `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` ( dataset_name, table_name, highwatermark )
values ( 'outbound','fact_roadside_service_call',null),
       ( 'outbound','insurance_customer_role',null),
       ( 'outbound','dim_demo_ins_score',null),
       ( 'outbound','fact_sales_header',null),
       ( 'outbound','fact_sales_detail',null),
       ( 'outbound','insurance_policy',null,
       ( 'outbound','insurance_policy_line',null),
       ( 'outbound','dim_contact_segmentation',null)
       ( 'outbound','sales_agent_activity',null);
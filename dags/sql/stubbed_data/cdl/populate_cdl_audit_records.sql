-- CDL audit records for Insurance data tables
-- SQL Server DB

insert into table dbo.cdl_load_status
(table_name, load_start_dtm, load_complete_dtm)
values ( 'fact_roadside_service_call',null,null),
       ( 'insurance_customer_role',null,null),
       ( 'dim_demo_ins_score',null,null),
       ( 'fact_sales_header',null,null),
       ( 'fact_sales_detail',null,null),
       ( 'insurance_policy',null,null),
       ( 'insurance_policy_line',null,null),
       ( 'dim_contact_segmentation',null,null)
       ( 'dim_product',null,null),
       ( 'dim_aca_office',null,null)
go
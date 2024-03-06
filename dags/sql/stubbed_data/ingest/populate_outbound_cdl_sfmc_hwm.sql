if (not exists(select * from `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` where dataset_name = 'outbound_cdl_sfmc' and table_name = 'sfmc_open')) then
  insert into `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` (dataset_name, table_name, highwatermark) values ('outbound_cdl_sfmc','sfmc_open','1900-01-01 00:00:00');
end if;
if (not exists(select * from `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` where dataset_name = 'outbound_cdl_sfmc' and table_name = 'sfmc_subscribers')) then
  insert into `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` (dataset_name, table_name, highwatermark) values ('outbound_cdl_sfmc','sfmc_subscribers','1900-01-01 00:00:00');
end if;
if (not exists(select * from `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` where dataset_name = 'outbound_cdl_sfmc' and table_name = 'sfmc_click')) then
  insert into `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` (dataset_name, table_name, highwatermark) values ('outbound_cdl_sfmc','sfmc_click','1900-01-01 00:00:00');
end if;
if (not exists(select * from `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` where dataset_name = 'outbound_cdl_sfmc' and table_name = 'sfmc_sendlog')) then
  insert into `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` (dataset_name, table_name, highwatermark) values ('outbound_cdl_sfmc','sfmc_sendlog','1900-01-01 00:00:00');
end if;
 delete from `{{ var.value.INGESTION_PROJECT }}.mzp.rider`
 where rider_ky in (select rider_ky from `{{ var.value.INGESTION_PROJECT }}.work.tmp_rider_delete`);
 
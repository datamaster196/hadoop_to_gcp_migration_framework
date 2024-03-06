CREATE OR REPLACE TABLE `{{ var.value.INGESTION_PROJECT }}.work.member_backup` as
SELECT * FROM `{{ var.value.INGESTION_PROJECT }}.mzp.member`;

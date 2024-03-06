CREATE OR REPLACE TABLE `{{ var.value.INGESTION_PROJECT }}.work.membership_backup` as
SELECT * FROM `{{ var.value.INGESTION_PROJECT }}.mzp.membership`;

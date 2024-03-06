CREATE OR REPLACE TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_work.dim_member_backup` as
SELECT * FROM `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_member`;

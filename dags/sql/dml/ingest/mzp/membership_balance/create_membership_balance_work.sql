  DROP TABLE IF EXISTS `{{ var.value.INGESTION_PROJECT }}.work.stg_membership_balance`;

  CREATE TABLE IF NOT EXISTS `{{ var.value.INGESTION_PROJECT }}.work.stg_membership_balance` (
    membership_ky			        STRING,
    amount_due				    	STRING,
    last_upd_dt				        STRING,
    adw_lake_insert_datetime        DATETIME,
    adw_lake_insert_batch_number    INT64
    )
  PARTITION BY DATE(_PARTITIONTIME)
  ;
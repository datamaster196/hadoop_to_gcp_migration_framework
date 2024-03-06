  MERGE INTO
    `{{ var.value.INGESTION_PROJECT }}.mzp.membership_balance` a
  USING
    `{{ var.value.INGESTION_PROJECT }}.work.stg_membership_balance` b
  ON
    (a.membership_ky = b.membership_ky )
    WHEN NOT MATCHED THEN INSERT (
    membership_ky,
    amount_due,
    last_upd_dt,
    adw_lake_insert_datetime,
    adw_lake_insert_batch_number ) VALUES (
    b.membership_ky,
    b.amount_due,
    b.last_upd_dt,
    b.adw_lake_insert_datetime,
    b.adw_lake_insert_batch_number
  )
    WHEN MATCHED
    THEN
  UPDATE
  SET
    a.amount_due = b.amount_due,
    a.last_upd_dt = b.last_upd_dt,
    a.adw_lake_insert_datetime = b.adw_lake_insert_datetime,
    a.adw_lake_insert_batch_number = b.adw_lake_insert_batch_number;

  UPDATE `{{ var.value.INGESTION_PROJECT }}.admin.ingestion_highwatermarks` SET highwatermark = (select coalesce(cast(max(PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', last_upd_dt)) as STRING), '')
  from `{{ var.value.INGESTION_PROJECT }}.mzp.membership_balance`) WHERE dataset_name = 'mzp' and table_name = 'membership_balance';
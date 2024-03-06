CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_member_solicitation_work_source` AS
SELECT
  solicitation_source.solicitation_ky,
  solicitation_source.solicitation_cd,
  solicitation_source.group_cd,
  solicitation_source.membership_type_cd,
  solicitation_source.campaign_cd,
  solicitation_source.solicitation_category_cd,
  solicitation_source.last_upd_dt,
  ROW_NUMBER() OVER(PARTITION BY solicitation_source.solicitation_ky ORDER BY solicitation_source.last_upd_dt DESC) AS dupe_check
FROM
  `{{ var.value.INGESTION_PROJECT }}.mzp.solicitation` solicitation_source
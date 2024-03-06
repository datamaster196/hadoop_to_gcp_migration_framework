WITH table_filter AS (
  SELECT
    aa.table_catalog,
    aa.table_schema,
    aa.table_name
  FROM (
    SELECT * FROM   `{{var.value.INTEGRATION_PROJECT}}.adw.INFORMATION_SCHEMA.TABLES`
    UNION ALL
    SELECT * FROM   `{{var.value.INTEGRATION_PROJECT}}.adw_pii.INFORMATION_SCHEMA.TABLES`
    ) AA
  JOIN (
  	SELECT DISTINCT
  		table_name AS sheet_table_name
  	FROM `{{var.value.INGESTION_PROJECT}}.admin.data_steward`) BB ON (
  	aa.table_name = bb.sheet_table_name )
), corrected_columns AS (
   SELECT
    aa.table_catalog,
    aa.table_schema,
    aa.table_name,
    aa.column_name AS name,
    aa.ordinal_position,
    aa.data_type AS type,
    coalesce(cc.column_description, 'TBD') as description,
    CASE WHEN aa.is_nullable = 'YES' THEN 'NULLABLE' ELSE 'REQUIRED' END AS mode
   FROM (
      SELECT * FROM `{{var.value.INTEGRATION_PROJECT}}.adw.INFORMATION_SCHEMA.COLUMNS`
      UNION ALL
      SELECT * FROM `{{var.value.INTEGRATION_PROJECT}}.adw_pii.INFORMATION_SCHEMA.COLUMNS`
   ) aa
   JOIN table_filter bb ON (aa.table_schema = bb.table_schema AND aa.table_name = bb.table_name)
   LEFT JOIN `{{var.value.INGESTION_PROJECT}}.admin.data_steward` cc ON (aa.table_name = cc.table_name AND aa.column_name = cc.column_name)
 ), completed_fields AS (
 SELECT
  table_catalog,
  table_schema,
  table_name,
  description,
  name,
  type,
  mode
 FROM corrected_columns)
SELECT
  table_catalog,
  table_schema,
  table_name,
  description,
  name,
  type,
  mode
FROM completed_fields
ORDER BY 1,2,3 ;

CREATE OR REPLACE FUNCTION
  `{{var.value.INTEGRATION_PROJECT}}.udfs.hash_phone` (s STRING) as ( (
    SELECT
      TO_BASE64(MD5(ifnull(s,
            ''))) ) );
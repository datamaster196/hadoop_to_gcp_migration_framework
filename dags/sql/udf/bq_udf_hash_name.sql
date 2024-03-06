CREATE OR REPLACE FUNCTION
  `{{var.value.INTEGRATION_PROJECT}}.udfs.hash_name`( first_name STRING,
    middle_name STRING,
    last_name STRING,
    name_suffix STRING,
    salutation STRING) as ( ( TO_BASE64(MD5(CONCAT(ifnull(first_name,
              ''),'|',ifnull(middle_name,
              ''),'|',ifnull(last_name,
              ''),'|',ifnull(name_suffix,
              ''),'|',ifnull(salutation,
              '')))) ) );
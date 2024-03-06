create or replace function `{{var.value.INTEGRATION_PROJECT}}.udfs.hash_address`(address_line1 STRING, address_line2 STRING, care_of_name STRING, city STRING, state STRING, zip STRING, zip4 STRING, country STRING) AS ((
    SELECT
      TO_BASE64(MD5(CONCAT(ifnull(address_line1,
              ''),'|',ifnull(address_line2,
              ''),'|',ifnull(care_of_name,
              ''),'|',ifnull(city,
              ''),'|',ifnull(state,
              ''),'|',ifnull(zip,
              ''),'|',ifnull(zip4,
              ''),'|',ifnull(country,
              '')))) ) );
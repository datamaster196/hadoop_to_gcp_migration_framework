 create or replace function `{{ var.value.INTEGRATION_PROJECT }}.udfs.standardize_string`(s STRING) AS ((
    select lower(trim(coalesce(s, '')))
    ))
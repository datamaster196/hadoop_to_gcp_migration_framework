CREATE OR REPLACE FUNCTION `{{ var.value.INTEGRATION_PROJECT }}.udfs.format_phone_number`(s STRING) AS ((
    (
    select case when CHARACTER_LENGTH(s) = 7 then concat(SUBSTR(s, 0,3), " - ", SUBSTR(s, 3, 4))
                when CHARACTER_LENGTH(s) = 10 then concat("(", SUBSTR(s, 0,3), ") - ", SUBSTR(s,4, 3), " - ", SUBSTR(s, 7,4))
                when CHARACTER_LENGTH(s) = 11 then concat(SUBSTR(s, 0,1), " - ", "(", SUBSTR(s, 1,3), ") - ", SUBSTR(s,5, 3), " - ", SUBSTR(s, 8,4))
    else "" end
    )
    ));
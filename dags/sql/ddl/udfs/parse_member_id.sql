  CREATE OR REPLACE FUNCTION `{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(mbr_id STRING, type STRING, calr_mbr_clb_cd STRING, assoc_mbr_id STRING) AS ((
    SELECT
    CASE     WHEN type='iso_cd' THEN
             if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
             ( CASE
                 WHEN LENGTH(TRIM(mbr_id))=16 THEN SUBSTR(mbr_id, 1, 3)
                 WHEN LENGTH(TRIM(mbr_id))=7 THEN CAST(NULL AS STRING)
                 WHEN LENGTH(TRIM(mbr_id))=9 THEN CAST(NULL AS STRING)
             END
             ))
             WHEN type='club_cd' THEN
             if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
             (CASE
                when length(trim(mbr_id))=16 then  SUBSTR(mbr_id, 4, 3)
                when length(trim(mbr_id))=7 then  calr_mbr_clb_cd
                when length(trim(mbr_id))=9 then  calr_mbr_clb_cd
            END
            ))
            WHEN type='membership_id' THEN
            if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
            (CASE
                when length(trim(mbr_id))=16 then SUBSTR(mbr_id, 7, 7)
                when length(trim(mbr_id))=7 then mbr_id
                when length(trim(mbr_id))=9 then SUBSTR(mbr_id, 1, 7)
            END
            ))
            WHEN type='associate_id' THEN
            if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
            (CASE
                when length(trim(mbr_id))=16 then SUBSTR(mbr_id, 14, 2)
                when length(trim(mbr_id))=7 then assoc_mbr_id
                when length(trim(mbr_id))=9 then SUBSTR(mbr_id, 8, 2)
            END
            ))
            WHEN type='check_digit_nr' THEN
            if(SAFE_CAST(mbr_id AS FLOAT64) is null,null,
            (CASE
                when  length(trim(mbr_id))=16 then SUBSTR(mbr_id, 16, 1)
                when length(trim(mbr_id))=7 then cast(null  as STRING)
                when length(trim(mbr_id))=9 then cast(null  as STRING)
            END
            ))

        END
        AS type_ret
        ));
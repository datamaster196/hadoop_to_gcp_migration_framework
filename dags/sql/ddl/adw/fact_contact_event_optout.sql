CREATE TABLE IF NOT EXISTS`{{ var.value.INTEGRATION_PROJECT }}.adw.fact_contact_event_optout`(
    contact_event_pref_adw_key       STRING        NOT NULL,
    contact_adw_key                  STRING        NOT NULL,
    source_system_key                STRING        NOT NULL,
    system_of_record                 STRING                ,
    topic_nm                         STRING                ,
    channel                          STRING                ,
    event_cd                         STRING                ,
    event_dtm                        DATETIME              ,
    integrate_insert_datetime        DATETIME      NOT NULL,
    integrate_insert_batch_number    INT64         NOT NULL,
    integrate_update_datetime        DATETIME      NOT NULL,
    integrate_update_batch_number    INT64         NOT NULL
)
;
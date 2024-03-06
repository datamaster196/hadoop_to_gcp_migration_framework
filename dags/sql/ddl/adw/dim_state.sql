CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_state`(
    state_adw_key                   STRING     NOT NULL,
    state_cd                        STRING,
    state_nm                        STRING,
    country_cd                      STRING,
    country_nm                      STRING,
    region_nm                       STRING,
    integrate_insert_datetime       DATETIME   NOT NULL,
    integrate_insert_batch_number   INT64      NOT NULL
)
;
CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_business_line`(
    biz_line_adw_key                 String   NOT NULL,
    biz_line_cd                      String   NOT NULL,
    biz_line_desc                    String   ,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            NOT NULL
)
;
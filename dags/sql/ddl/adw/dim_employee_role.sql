CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.dim_employee_role`(
   emp_role_adw_key                 String   NOT NULL,
    emp_adw_key                      String   NOT NULL,
    nm_adw_key                       String      NOT NULL,
    aca_office_adw_key               String   NOT NULL,
    emp_biz_role_line_cd             String   ,
    emp_role_id                      String   ,
    emp_role_typ                     String   ,
    effective_start_datetime         datetime       NOT NULL,
    effective_end_datetime           datetime       NOT NULL,
    actv_ind                         String       NOT NULL,
    adw_row_hash                     String      ,
    integrate_insert_datetime        datetime       NOT NULL,
    integrate_insert_batch_number    int64            NOT NULL,
    integrate_update_datetime        datetime       NOT NULL,
    integrate_update_batch_number    int64            NOT NULL
)
;
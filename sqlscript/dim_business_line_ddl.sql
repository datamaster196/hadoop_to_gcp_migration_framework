
CREATE TABLE adw.dim_business_line(
    business_line_adw_key            STRING    NOT NULL,
    business_line_code               STRING    NOT NULL,
    business_line_description        STRING,
    integrate_insert_datetime        DATETIME           NOT NULL,
    integrate_insert_batch_number    INT64        NOT NULL
)
;

CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.fact_sales_header`(
    sales_header_adw_key            String NOT NULL Options(description='Unique key for each Sales Header record.'),
    aca_office_adw_key              String NOT NULL Options(description='The foreign key reference to the dim_aca_office table.'),
    emp_adw_key                     String NOT NULL Options(description='The foreign key reference to the dim_employee table.'),
    contact_adw_key                 String NOT NULL Options(description='The foreign key reference to the dim_contact_info table.'),
    source_system                   String Options(description='This is the source system from which these sales are coming from.  Examples are POS, VAST, GlobalWare etc.') ,
    source_system_key               String Options(description='The source system record ID uniquely identifying the record.') ,
    sale_adw_key_dt                 String Options(description='The foreign key reference to Dim Date for the date of the sale.') ,
    sale_dtm                        datetime Options(description='The date of the sale.') ,
    receipt_nbr                     String Options(description='This is the receipt number printed on the invoice / receipt.') ,
    sale_void_ind                   String Options(description='This indicates if the Sale was voided.') ,
    return_ind                      String Options(description='This indicates of the Invoice was a return') ,
    total_sale_price                numeric Options(description='This is the total sale price on the receipt') ,
    total_labor_cost                numeric Options(description='This is the total labor cost portion of the sale.') ,
    total_cost                      numeric Options(description='This is the total cost of the goods sold.') ,
    total_tax                       numeric Options(description='This is the total sales tax for the receipt.') ,
    effective_start_datetime        datetime  NOT NULL Options(description='Date that this record is effectively starting'),
    effective_end_datetime          datetime  NOT NULL Options(description='Date that this record is effectively ending'),
    actv_ind                        String    NOT NULL Options(description='Indicates if the record is currently the active record'),
    adw_row_hash                    String    NOT NULL Options(description='Hash Key of appropriate fields to enable easier type 2 logic'),
    integrate_insert_datetime       datetime  NOT NULL Options(description='Datetime that this record was first inserted'),
    integrate_insert_batch_number   int64     NOT NULL Options(description='Batch that first inserted this record'),
    integrate_update_datetime       datetime  NOT NULL Options(description='Most recent datetime that this record was updated'),
    integrate_update_batch_number   int64     NOT NULL Options(description='Most recent batch that updated this record')
)
PARTITION BY _PARTITIONDATE
options(description="Conformed Fact header table for all invoice details common to a particular receipt.  The natural key is the Source System, Source System Key, Receipt Number and the ACA Office ADW Key")
;
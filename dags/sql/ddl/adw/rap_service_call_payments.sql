CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw.rap_service_call_payments`(
	tran_typ_cd		            STRING NOT NULL OPTIONS(description="3 character code representing the type of transaction. LCL = Late close; PDC = Paid Club; CAN = Canceled"),
	service_call_dt		        DATE NOT NULL OPTIONS(description="The date the service was provided"),
	rap_call_id		            STRING NOT NULL OPTIONS(description="8 digit call number assigned by the RAP call center"),
	contact_adw_key		        STRING NOT NULL OPTIONS(description="FK to dim_contact_info"),
	service_club_cd		        STRING OPTIONS(description="3 character code representing the club that performed the service"),
	rap_program_cd		        STRING OPTIONS(description="3 character code representing the RAP organization"),
	service_call_tow_miles	    NUMERIC OPTIONS(description="The number of miles the vehicle was towed"),
	total_payment_amt	        NUMERIC OPTIONS(description="The total payable amount for the services provided"),
	update_dt		            DATE NOT NULL OPTIONS(description="Calculated date based on the file name. Month and year is first extracted from the file name, then date is computed as the 5th day of the following month"),
	integrate_insert_datetime	    DATETIME NOT NULL OPTIONS(description="Date and time the record was inserted"),
	integrate_insert_batch_number	INT64 NOT NULL OPTIONS(description="Batch that created the record")
)
OPTIONS(description="Amount being paid to the club for performing RAP services. Primarily used perform their reconciliation of which calls were paid.");
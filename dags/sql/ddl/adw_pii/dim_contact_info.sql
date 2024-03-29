CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info`(
    contact_adw_key                  String NOT NULL Options(description='Unique key created for each contact info record'),
    contact_typ_cd                   String Options(description='Code that indicates if contact is a person, organization, etc.'),
    contact_first_nm                 String Options(description='Default first name as determined by primacy rules'),
    contact_middle_nm                String Options(description='Default middle name as determined by primacy rules'),
    contact_last_nm                  String Options(description='Default last name as determined by primacy rules'),
    contact_suffix_nm                String Options(description='Default name suffix as determined by primacy rules'),
    contact_title_nm                 String Options(description='Default title as determined by primacy rules'),
    contact_address_1_nm             String Options(description='Default first line of address as determined by primacy rules'),
    contact_address_2_nm             String Options(description='Default second line of address as determined by primacy rules'),
    contact_care_of_nm               String Options(description='Default care of name as determined by primacy rules'),
    contact_city_nm                  String Options(description='Default city as determined by primacy rules'),
    contact_state_cd                 String Options(description='Default 2 letter code for states as determined by primacy rules'),
    contact_state_nm                 String Options(description='Default full state name as determined by primacy rules'),
    contact_postal_cd                String Options(description='Default postal code, specifically first 5 digits if US, as determined by primacy rules'),
    contact_postal_plus_4_cd         String Options(description='Default postal code plus four as determined by primacy rules'),
    contact_country_nm               String Options(description='Default country as determined by primacy rules'),
    contact_phone_nbr                String Options(description='Default raw phone number as determined by primacy rules'),
    contact_phone_formatted_nbr      String Options(description='Formatted default phone number as determined by primacy rules'),
    contact_phone_extension_nbr      int64 Options(description='Extension portion of the phone number determined by the primacy rules'),
    contact_email_nm                 String Options(description='Default email address as determined by the primacy rules'),
    contact_gender_cd                String Options(description='Gender Code for this contact info record'),
    contact_gender_nm                String Options(description='Full name of gender for this contact info record'),
    contact_birth_dt                 date Options(description='Date of birth of this contact info record'),
    primacy_source_system_nm         String Options(description='The name of the business line source system from which this record was created.'),
    adw_row_hash                     String NOT NULL Options(description='Hash Key of appropriate fields to enable easier type 2 logic'),
    integrate_insert_datetime        datetime NOT NULL Options(description='Datetime that this record was first inserted'),
    integrate_insert_batch_number    int64 NOT NULL Options(description='Batch that first inserted this record'),
    integrate_update_datetime        datetime NOT NULL Options(description='Most recent datetime that this record was updated'),
    integrate_update_batch_number    int64 NOT NULL Options(description='Most recent batch that updated this record')
)
options(description="This table is a unique records for the following: - Each person or business (Commercial Lines Insurance, Corporate Travel Clients) who has either currently or previously transacted business with us for sales and services as a customer.   - Future Prospects coming from various sources such as Market Magnifier, Direct Mail Prospect List etc.- Also included are vendors from whom we buy goods to sell to the customer or service providers such as Insurance Carrier, Travel providers and Roadside Assistance facilities which provide services to the customer on our behalf.")

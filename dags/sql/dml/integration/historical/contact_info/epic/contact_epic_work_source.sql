CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.epic_contact_source_work` AS
select
epic.iso_cd,
epic.club_cd,
epic.membership_id,
epic.associate_id,
epic.check_digit_nr,
epic.client_unique_entity,
epic.gender,
epic.birthdate,
upper(coalesce(name.cleansed_first_nm,name.raw_first_nm,'')) as cleansed_first_nm,
upper(coalesce(name.cleansed_last_nm,name.raw_last_nm, '')) as cleansed_last_nm,
upper(coalesce(name.cleansed_suffix_nm,name.raw_suffix_nm, '')) as cleansed_suffix_nm,
upper(coalesce(address.cleansed_address_1_nm,address.raw_address_1_nm, '')) as cleansed_address_1_nm,
upper(coalesce(address.cleansed_city_nm,address.raw_city_nm, '')) as cleansed_city_nm,
upper(coalesce(address.cleansed_state_cd,address.raw_state_cd, '')) as cleansed_state_cd,
upper(coalesce(address.cleansed_postal_cd,address.raw_postal_cd, '')) as cleansed_postal_cd,
upper(coalesce(email.cleansed_email_nm,email.raw_email_nm, '')) as cleansed_email_nm,
epic.last_upd_dt,
epic.address_adw_key,
epic.phone_adw_key,
epic.nm_adw_key,
epic.email_adw_key
from
(
SELECT
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(contactnumber.descriptionof,'iso_cd', contactnumber.descriptionof, contactnumber.descriptionof) iso_cd,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(contactnumber.descriptionof,'club_cd', contactnumber.descriptionof, contactnumber.descriptionof) club_cd,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(contactnumber.descriptionof,'membership_id', contactnumber.descriptionof, contactnumber.descriptionof) membership_id,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(contactnumber.descriptionof,'associate_id', contactnumber.descriptionof, contactnumber.descriptionof) associate_id,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_member_id`(contactnumber.descriptionof,'check_digit_nr', contactnumber.descriptionof, contactnumber.descriptionof) check_digit_nr,
  client.uniqentity AS client_unique_entity,
  coalesce(TRIM(contactname.gendercode),'') AS gender,
  SAFE_CAST(SUBSTR(contactname.birthdate,1,10) AS date) AS birthdate,
  GREATEST( coalesce(client.effective_start_date,
      datetime('2000-01-01')), coalesce(contactname.effective_start_date,
      datetime('2000-01-01')), coalesce(contactaddress.effective_start_date,
      datetime('2000-01-01')), coalesce(contactnumber.effective_start_date,
      datetime('2000-01-01')) ) AS last_upd_dt,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_address`(coalesce(TRIM(contactaddress.address1),''), coalesce(TRIM(contactaddress.address2),''), '', coalesce(TRIM(contactaddress.city),''), coalesce(TRIM(contactaddress.cdstatecode),''), coalesce(TRIM(contactaddress.postalcode),''), '', coalesce(TRIM(contactaddress.cdcountrycode),'')) as address_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_name`(coalesce(TRIM(contactname.firstname),''), coalesce(TRIM(contactname.middlename),''), coalesce(TRIM(contactname.lastname),''), coalesce(TRIM(contactname.lksuffix),''), coalesce(TRIM(contactname.lkprefix),'')) as nm_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_phone`(coalesce(TRIM(contactnumber.number),'')) as phone_adw_key,
  `{{ var.value.INTEGRATION_PROJECT }}.udfs.hash_email`(coalesce(TRIM(contactnumber.emailweb),'')) as email_adw_key   
FROM (
  SELECT
    *,
    CAST(SUBSTR(coalesce(updateddate,
          ts), 1, 23) AS datetime) AS effective_start_date,
    CAST(coalesce(LEAD(SUBSTR(coalesce(updateddate,
              ts), 1, 23)) OVER (PARTITION BY uniqentity ORDER BY coalesce(updateddate, ts) ASC),
        '9999-12-31') AS datetime) AS effective_end_date
  FROM
    `{{ var.value.INGESTION_PROJECT }}.epic.client`
  WHERE
    uniqentity<>'-1') client
LEFT JOIN (
  SELECT
    *,
    CAST(SUBSTR(coalesce(updateddate,
          ts), 1, 23) AS datetime) AS effective_start_date,
    CAST(coalesce(LEAD(SUBSTR(coalesce(updateddate,
              ts), 1, 23)) OVER (PARTITION BY uniqcontactname ORDER BY coalesce(updateddate, ts) ASC),
        '9999-12-31') AS datetime) AS effective_end_date
  FROM
    `{{ var.value.INGESTION_PROJECT }}.epic.contactname`) contactname
ON
  client.uniqcontactnameprimary=contactname.uniqcontactname
  AND client.effective_end_date>=contactname.effective_start_date
  AND client.effective_start_date<contactname.effective_end_date
LEFT JOIN (
  SELECT
    *,
    CAST(SUBSTR(coalesce(updateddate,
          inserteddate), 1, 23) AS datetime) AS effective_start_date,
    CAST(coalesce(LEAD(SUBSTR(coalesce(updateddate,
              inserteddate), 1, 23)) OVER (PARTITION BY uniqcontactaddress ORDER BY coalesce(updateddate, inserteddate) ASC),
        '9999-12-31') AS datetime) AS effective_end_date
  FROM
    `{{ var.value.INGESTION_PROJECT }}.epic.contactaddress`) contactaddress
ON
  client.uniqcontactaddressaccount=contactaddress.uniqcontactaddress
  AND client.effective_end_date>=contactaddress.effective_start_date
  AND client.effective_start_date<contactaddress.effective_end_date
LEFT JOIN (
  SELECT
    *,
    CAST(SUBSTR(coalesce(updateddate,
          inserteddate), 1, 23) AS datetime) AS effective_start_date,
    CAST(coalesce(LEAD(SUBSTR(coalesce(updateddate,
              inserteddate), 1, 23)) OVER (PARTITION BY uniqcontactnumber ORDER BY coalesce(updateddate, inserteddate) ASC),
        '9999-12-31') AS datetime) AS effective_end_date
  FROM
    `{{ var.value.INGESTION_PROJECT }}.epic.contactnumber`) contactnumber
ON
  client.uniqcontactnumberaccount=contactnumber.uniqcontactnumber
  AND client.effective_end_date>=contactnumber.effective_start_date
  AND client.effective_start_date<contactnumber.effective_end_date
) epic
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_name` name on name.nm_adw_key=epic.nm_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` address on address.address_adw_key=epic.address_adw_key
left outer join `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_email` email on email.email_adw_key=epic.email_adw_key
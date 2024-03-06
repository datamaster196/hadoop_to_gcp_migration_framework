DELETE FROM `{{ var.value.INTEGRATION_PROJECT }}.cdl.recipient` WHERE 1=1;
INSERT INTO `{{ var.value.INTEGRATION_PROJECT }}.cdl.recipient`
  (iGender,,sAddress1,sAddress2,sCity,sCountryCode,sEmail,sFirstName,sLastName,sMiddleName,--sOrigin,
   sPhone,sSalutation,sStateCode,sZipCode,tsAddrLastCheck,tsBirth,
   contact_adw_key)

select  a.contact_gender_cd,
        a.contact_address_1_nm,
        a.contact_address_2_nm,
        a.contact_city_nm,
        a.contact_country_nm,
        a.contact_email_nm,
        a.contact_first_nm,
        a.contact_last_nm,
        a.contact_middle_nm,
        --primacy_source_system_nm
        a.contact_phone_nbr,
        a.contact_title_nm,
        a.contact_state_cd,
        a.contact_postal_cd,
        c.integrate_update_datetime,
        a.contact_birth_dt,
        a.contact_adw_key
from `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_contact_info` a left join
     (
      select contact_adw_key ,  address_Adw_key,
            ROW_NUMBER() over (PARTITION BY contact_adw_key order by effective_start_datetime desc) DUPE_CHECK
      FROM  `{{ var.value.INTEGRATION_PROJECT }}.adw.xref_contact_address`
      where actv_ind = 'Y'
    ) b on a.contact_adw_key = b.contact_adw_key and  b.dupe_check = 1 left join
     `{{ var.value.INTEGRATION_PROJECT }}.adw_pii.dim_address` c on b.address_adw_key = c.address_adw_key
;


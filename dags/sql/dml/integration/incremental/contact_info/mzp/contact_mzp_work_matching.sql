--initial matching to the target
CREATE OR REPLACE TABLE `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_matched` AS
SELECT
  coalesce(member_ky_match.contact_adw_key,
    address_match.contact_adw_key,
    zip5_match.contact_adw_key,
    email_match.contact_adw_key) AS contact_adw_key,
  source.iso_cd,
  source.club_cd,
  source.membership_id,
  source.associate_id,
  source.check_digit_nr,
  source.membership_ky,
  source.member_ky,
  source.derived_member_ky,
  source.customer_id,
  source.gender,
  source.birthdate,
  source.last_upd_dt,
  source.cleansed_first_nm,
  source.cleansed_last_nm,
  source.cleansed_suffix_nm,
  source.cleansed_address_1_nm,
  source.cleansed_city_nm,
  source.cleansed_state_cd,
  source.cleansed_postal_cd,
  source.cleansed_email_nm,
  source.address_adw_key,
  source.nm_adw_key,
  source.phone_adw_key,
  source.email_adw_key
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_source_work` source
  -- Matching Criteria 1: Client Unique Entity --
LEFT OUTER JOIN (
  SELECT
    MAX(contact_adw_key) AS contact_adw_key,
    derived_member_ky
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
  WHERE
    coalesce(derived_member_ky,'') != ''
  GROUP BY
    derived_member_ky ) member_ky_match
ON
  source.derived_member_ky=member_ky_match.derived_member_ky
  -- Matching Criteria 2: First Name, Last Name, Address Line 1, City, State, Name Suffix --
LEFT OUTER JOIN (
  SELECT
    MAX(contact_adw_key) AS contact_adw_key,
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_address_1_nm,
    cleansed_city_nm,
    cleansed_state_cd
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
  WHERE
    coalesce(cleansed_city_nm,
      '') != ''
  GROUP BY
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_address_1_nm,
    cleansed_city_nm,
    cleansed_state_cd ) address_match
ON
  source.cleansed_first_nm = address_match.cleansed_first_nm
  AND source.cleansed_last_nm = address_match.cleansed_last_nm
  AND source.cleansed_suffix_nm = address_match.cleansed_suffix_nm
  AND source.cleansed_address_1_nm = address_match.cleansed_address_1_nm
  AND source.cleansed_city_nm = address_match.cleansed_city_nm
  AND source.cleansed_state_cd = address_match.cleansed_state_cd
  AND source.cleansed_first_nm != ''
  AND source.cleansed_last_nm != ''
  AND source.cleansed_address_1_nm != ''
  AND source.cleansed_city_nm != ''
  AND source.cleansed_state_cd != ''
  -- Matching Criteria 3: First Name, Last Name, Address Line 1, Zip-5, Name Suffix --
LEFT OUTER JOIN (
  SELECT
    MAX(contact_adw_key) AS contact_adw_key,
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_address_1_nm,
    cleansed_postal_cd
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
  WHERE
    coalesce(cleansed_postal_cd,
      '') != ''
  GROUP BY
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_address_1_nm,
    cleansed_postal_cd ) zip5_match
ON
  source.cleansed_first_nm = zip5_match.cleansed_first_nm
  AND source.cleansed_last_nm = zip5_match.cleansed_last_nm
  AND source.cleansed_suffix_nm = zip5_match.cleansed_suffix_nm
  AND source.cleansed_address_1_nm = zip5_match.cleansed_address_1_nm
  AND source.cleansed_postal_cd = zip5_match.cleansed_postal_cd
  AND source.cleansed_first_nm != ''
  AND source.cleansed_last_nm != ''
  AND source.cleansed_address_1_nm != ''
  AND source.cleansed_postal_cd != ''
  -- Matching Criteria 4: First Name, Last Name, Email, Name Suffix --
LEFT OUTER JOIN (
  SELECT
    MAX(contact_adw_key) AS contact_adw_key,
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_email_nm
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_target_work`
  WHERE
    coalesce(cleansed_email_nm,
      '') != ''
  GROUP BY
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_email_nm ) email_match
ON
  source.cleansed_first_nm = email_match.cleansed_first_nm
  AND source.cleansed_last_nm = email_match.cleansed_last_nm
  AND source.cleansed_suffix_nm = email_match.cleansed_suffix_nm
  AND source.cleansed_email_nm = email_match.cleansed_email_nm
  AND source.cleansed_first_nm != ''
  AND source.cleansed_last_nm != ''
  AND source.cleansed_email_nm != ''
;
      -- Update to make sure that all entries with the same member_ky are tied to the same contact --  


UPDATE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_matched` target
SET
  target.contact_adw_key=source.contact_adw_key
FROM (
  SELECT
    derived_member_ky,
    MAX(contact_adw_key) contact_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_matched`
  GROUP BY
    1) source
WHERE
  target.derived_member_ky=source.derived_member_ky
  AND source.contact_adw_key IS NOT NULL  
;

--self matching on source key
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` AS
WITH
  member_guid AS (
  SELECT
    GENERATE_UUID() AS contact_adw_key,
    derived_member_ky
  FROM (
    SELECT
      DISTINCT derived_member_ky
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_matched`
    WHERE
      contact_adw_key IS NULL) )
SELECT
  b.contact_adw_key AS contact_adw_key,
  original.membership_ky,
  original.member_ky,
  original.derived_member_ky,
  original.customer_id,
  original.iso_cd,
  original.club_cd,
  original.membership_id,
  original.associate_id,
  original.check_digit_nr,
  original.gender,
  original.birthdate,
  original.last_upd_dt,
  original.cleansed_first_nm,
  original.cleansed_last_nm,
  original.cleansed_suffix_nm,
  original.cleansed_address_1_nm,
  original.cleansed_city_nm,
  original.cleansed_state_cd,
  original.cleansed_postal_cd,
  original.cleansed_email_nm,
  original.address_adw_key,
  original.nm_adw_key,
  original.phone_adw_key,
  original.email_adw_key
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_matched` original
LEFT OUTER JOIN
  member_guid b
ON
  original.derived_member_ky=b.derived_member_ky
WHERE
  original.contact_adw_key IS NULL
;
--self matching on name and address_match
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_2` AS
WITH
  address_guid AS (
  SELECT
    DISTINCT contact_adw_key,
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_address_1_nm,
    cleansed_city_nm,
    cleansed_state_cd
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1`
  WHERE
    cleansed_first_nm != ''
    AND cleansed_last_nm != ''
    AND cleansed_address_1_nm != ''
    AND cleansed_city_nm != ''
    AND cleansed_state_cd != '' ),
  matched_guid AS (
  SELECT
    a.contact_adw_key AS orig_contact_adw_key,
    MIN(b.contact_adw_key) AS new_contact_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` a
  JOIN
    address_guid b
  ON
    a.cleansed_first_nm = b.cleansed_first_nm
    AND a.cleansed_last_nm = b.cleansed_last_nm
    AND a.cleansed_suffix_nm = b.cleansed_suffix_nm
    AND a.cleansed_address_1_nm = b.cleansed_address_1_nm
    AND a.cleansed_city_nm = b.cleansed_city_nm
    AND a.cleansed_state_cd = b.cleansed_state_cd
  GROUP BY
    a.contact_adw_key )
SELECT
  coalesce(matched.new_contact_adw_key,
    contact_adw_key) AS contact_adw_key,
  original.membership_ky,
  original.member_ky,
  original.derived_member_ky,
  original.customer_id,
  original.iso_cd,
  original.club_cd,
  original.membership_id,
  original.associate_id,
  original.check_digit_nr,
  original.gender,
  original.birthdate,
  original.last_upd_dt,
  original.cleansed_first_nm,
  original.cleansed_last_nm,
  original.cleansed_suffix_nm,
  original.cleansed_address_1_nm,
  original.cleansed_city_nm,
  original.cleansed_state_cd,
  original.cleansed_postal_cd,
  original.cleansed_email_nm,
  original.address_adw_key,
  original.nm_adw_key,
  original.phone_adw_key,
  original.email_adw_key
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_1` original
LEFT OUTER JOIN
  matched_guid matched
ON
  original.contact_adw_key=matched.orig_contact_adw_key
;
--self matching on name address with zip
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_3` AS
WITH
  address_guid AS (
  SELECT
    DISTINCT contact_adw_key,
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_suffix_nm,
    cleansed_address_1_nm,
    cleansed_postal_cd
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_2`
  WHERE
    cleansed_first_nm != ''
    AND cleansed_last_nm != ''
    AND cleansed_address_1_nm != ''
    AND cleansed_postal_cd != '' ),
  matched_guid AS (
  SELECT
    a.contact_adw_key AS orig_contact_adw_key,
    MIN(b.contact_adw_key) AS new_contact_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_2` a
  JOIN
    address_guid b
  ON
    a.cleansed_first_nm = b.cleansed_first_nm
    AND a.cleansed_last_nm = b.cleansed_last_nm
    AND a.cleansed_suffix_nm = b.cleansed_suffix_nm
    AND a.cleansed_address_1_nm = b.cleansed_address_1_nm
    AND a.cleansed_postal_cd = b.cleansed_postal_cd
  GROUP BY
    a.contact_adw_key )
SELECT
  coalesce(matched.new_contact_adw_key,
    contact_adw_key) AS contact_adw_key,
  original.membership_ky,
  original.member_ky,
  original.derived_member_ky,
  original.customer_id,
  original.iso_cd,
  original.club_cd,
  original.membership_id,
  original.associate_id,
  original.check_digit_nr,
  original.gender,
  original.birthdate,
  original.last_upd_dt,
  original.cleansed_first_nm,
  original.cleansed_last_nm,
  original.cleansed_suffix_nm,
  original.cleansed_address_1_nm,
  original.cleansed_city_nm,
  original.cleansed_state_cd,
  original.cleansed_postal_cd,
  original.cleansed_email_nm,
  original.address_adw_key,
  original.nm_adw_key,
  original.phone_adw_key,
  original.email_adw_key
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_2` original
LEFT OUTER JOIN
  matched_guid matched
ON
  original.contact_adw_key=matched.orig_contact_adw_key
;
--self matching on name and email_match
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_4` AS
WITH
  email_guid AS (
  SELECT
    GENERATE_UUID() AS contact_adw_key,
    cleansed_first_nm,
    cleansed_last_nm,
    cleansed_email_nm,
    cleansed_suffix_nm
  FROM (
    SELECT
      DISTINCT cleansed_first_nm,
      cleansed_last_nm,
      cleansed_email_nm,
      cleansed_suffix_nm
    FROM
      `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_3`
    WHERE
      cleansed_first_nm != ''
      AND cleansed_last_nm != ''
      AND cleansed_email_nm != '') ),
  matched_guid AS (
  SELECT
    a.contact_adw_key AS orig_contact_adw_key,
    MIN(b.contact_adw_key ) AS new_contact_adw_key
  FROM
    `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_3` a
  JOIN
    email_guid b
  ON
    a.cleansed_first_nm = b.cleansed_first_nm
    AND a.cleansed_last_nm = b.cleansed_last_nm
    AND a.cleansed_email_nm = b.cleansed_email_nm
    AND a.cleansed_suffix_nm = b.cleansed_suffix_nm
  GROUP BY
    a.contact_adw_key )
SELECT
  coalesce(matched.new_contact_adw_key,
    contact_adw_key) AS contact_adw_key,
  original.membership_ky,
  original.member_ky,
  original.derived_member_ky,
  original.customer_id,
  original.iso_cd,
  original.club_cd,
  original.membership_id,
  original.associate_id,
  original.check_digit_nr,
  original.gender,
  original.birthdate,
  original.last_upd_dt,
  original.cleansed_first_nm,
  original.cleansed_last_nm,
  original.cleansed_suffix_nm,
  original.cleansed_address_1_nm,
  original.cleansed_city_nm,
  original.cleansed_state_cd,
  original.cleansed_postal_cd,
  original.cleansed_email_nm,
  original.address_adw_key,
  original.nm_adw_key,
  original.phone_adw_key,
  original.email_adw_key
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_3` original
LEFT OUTER JOIN
  matched_guid matched
ON
  original.contact_adw_key = matched.orig_contact_adw_key
;
--final staging
CREATE OR REPLACE TABLE
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_stage` AS
SELECT
  contact_adw_key,
  membership_ky,
  member_ky,
  derived_member_ky,
  customer_id,
  iso_cd,
  club_cd,
  membership_id,
  associate_id,
  check_digit_nr,
  nm_adw_key,
  address_adw_key,
  phone_adw_key,
  email_adw_key,
  gender,
  birthdate,
  last_upd_dt,
  cleansed_first_nm,
  cleansed_last_nm,
  cleansed_suffix_nm,
  cleansed_address_1_nm,
  cleansed_city_nm,
  cleansed_state_cd,
  cleansed_postal_cd,
  cleansed_email_nm
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_matched`
WHERE
  contact_adw_key IS NOT NULL
UNION ALL
SELECT
  contact_adw_key,
  membership_ky,
  member_ky,
  derived_member_ky,
  customer_id,
  iso_cd,
  club_cd,
  membership_id,
  associate_id,
  check_digit_nr,
  nm_adw_key,
  address_adw_key,
  phone_adw_key,
  email_adw_key,
  gender,
  birthdate,
  last_upd_dt,
  cleansed_first_nm,
  cleansed_last_nm,
  cleansed_suffix_nm,
  cleansed_address_1_nm,
  cleansed_city_nm,
  cleansed_state_cd,
  cleansed_postal_cd,
  cleansed_email_nm
FROM
  `{{ var.value.INTEGRATION_PROJECT }}.adw_work.mzp_contact_unmatched_work_4`
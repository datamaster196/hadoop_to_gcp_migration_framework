INSERT INTO
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_state` ( state_adw_key,
    state_cd,
    state_nm,
    country_cd,
    country_nm,
    region_nm,
    integrate_insert_datetime,
    integrate_insert_batch_number )
SELECT
  TO_BASE64(MD5(CONCAT( ifnull(a.state_cd,''),'|', ifnull(a.state_nm,'') ))) AS state_adw_key ,
  a.state_cd,
  a.state_nm,
  a.country_cd,
  a.country_nm,
  a.region_nm,
  a.integrate_insert_datetime,
  a.integrate_insert_batch_number
FROM (
   
  SELECT
     'AB' AS state_cd,
    'Alberta' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'BC' AS state_cd,
    'British Columbia' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MB' AS state_cd,
    'Manitoba' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NB' AS state_cd,
    'New Brunswick' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NF' AS state_cd,
    'Newfoundland' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NL' AS state_cd,
    'Newfoundland and Labrador' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NT' AS state_cd,
    'Northwest Territories' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NS' AS state_cd,
    'Nova Scotia' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NU' AS state_cd,
    'Nunavut' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'ON' AS state_cd,
    'Ontario' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'PE' AS state_cd,
    'Prince Edward Island' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'QC' AS state_cd,
    'Quebec' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'SK' AS state_cd,
    'Saskatchewan' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'YT' AS state_cd,
    'Yukon Territory' AS state_nm,
    'CAN' AS country_cd,
    'Canada' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'PR' AS state_cd,
    'Puerto Rico' AS state_nm,
    'PR' AS country_cd,
    'Puerto Rico' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'UK' AS state_cd,
    'United Kingdom' AS state_nm,
    'UK' AS country_cd,
    'United Kingdom' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'UN' AS state_cd,
    'Unknown' AS state_nm,
    'UNK' AS country_cd,
    'Unknown' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'IN' AS state_cd,
    'Indiana' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Central' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'KY' AS state_cd,
    'Kentucky' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Central' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'OH' AS state_cd,
    'Ohio' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Central' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'WV' AS state_cd,
    'West Virginia' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Central' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'CT' AS state_cd,
    'Connecticut' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Eastern' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'DE' AS state_cd,
    'Delaware' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Eastern' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'DC' AS state_cd,
    'District of Columbia' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Eastern' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MD' AS state_cd,
    'Maryland' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Eastern' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NJ' AS state_cd,
    'New Jersey' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Eastern' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'PA' AS state_cd,
    'Pennsylvania' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Eastern' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'VA' AS state_cd,
    'Virginia' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Eastern' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'KS' AS state_cd,
    'Kansas' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Great Plains' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'OK' AS state_cd,
    'Oklahoma' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Great Plains' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'SD' AS state_cd,
    'South Dakota' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Great Plains' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'AL' AS state_cd,
    'Alabama' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'AK' AS state_cd,
    'Alaska' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'AZ' AS state_cd,
    'Arizona' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'AR' AS state_cd,
    'Arkansas' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'CA' AS state_cd,
    'California' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'CO' AS state_cd,
    'Colorado' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'FL' AS state_cd,
    'Florida' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'GA' AS state_cd,
    'Georgia' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'HI' AS state_cd,
    'Hawaii' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'ID' AS state_cd,
    'Idaho' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'IL' AS state_cd,
    'Illinois' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'IA' AS state_cd,
    'Iowa' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'LA' AS state_cd,
    'Louisiana' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'ME' AS state_cd,
    'Maine' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MA' AS state_cd,
    'Massachusetts' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MI' AS state_cd,
    'Michigan' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'AA' AS state_cd,
    'Military' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'AE' AS state_cd,
    'Military' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'AP' AS state_cd,
    'Military' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'HA' AS state_cd,
    'Military' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MN' AS state_cd,
    'Minnesota' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MS' AS state_cd,
    'Mississippi' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MO' AS state_cd,
    'Missouri' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'MT' AS state_cd,
    'Montana' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NE' AS state_cd,
    'Nebraska' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NV' AS state_cd,
    'Nevada' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NH' AS state_cd,
    'New Hampshire' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NM' AS state_cd,
    'New Mexico' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NY' AS state_cd,
    'New York' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'NC' AS state_cd,
    'North Carolina' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'ND' AS state_cd,
    'North Dakota' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'OR' AS state_cd,
    'Oregon' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'RI' AS state_cd,
    'Rhode Island' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'SC' AS state_cd,
    'South Carolina' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'TN' AS state_cd,
    'Tennesse' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'TX' AS state_cd,
    'Texas' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'UT' AS state_cd,
    'Utah' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'VT' AS state_cd,
    'Vermont' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'WA' AS state_cd,
    'Washington' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'WI' AS state_cd,
    'Wisconsin' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
  UNION ALL
  SELECT
     'WY' AS state_cd,
    'Wyoming' AS state_nm,
    'USA' AS country_cd,
    'United States' AS country_nm,
    'Unknown' AS region_nm,
    CURRENT_DATETIME AS integrate_insert_datetime,
    {{ dag_run.id }} AS integrate_insert_batch_number
    ) a
WHERE
  0 = (
  SELECT
    COUNT(state_adw_key)
  FROM
    `{{var.value.INTEGRATION_PROJECT}}.adw.dim_state`);

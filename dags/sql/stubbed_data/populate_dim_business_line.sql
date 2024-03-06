-- Insert only when the table is empty
INSERT INTO
  `{{var.value.INTEGRATION_PROJECT}}.adw.dim_business_line` ( biz_line_adw_key,
    biz_line_cd,
    biz_line_desc,
    integrate_insert_datetime,
    integrate_insert_batch_number )
select a.biz_line_adw_key,
  a.biz_line_cd,
  a.biz_line_desc,
  a.integrate_insert_datetime,
  a.integrate_insert_batch_number
from
(
   Select '8b5e22eb-570a-45db-ae20-6fb30e3e5204' as biz_line_adw_key ,
   'ATO' as biz_line_cd,
   'Automotive Solutions' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '292584af-57f8-4d8c-912f-7c12df811fc1' as biz_line_adw_key ,
   'CC' as biz_line_cd,
   'Car Care' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '328c05c2-8b5b-4570-92a7-07338add8474' as biz_line_adw_key ,
   'CL' as biz_line_cd,
   'Commercial Insurance' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select 'ed7da038-43b0-443a-92fa-e035941b1ba6' as biz_line_adw_key ,
   'CT' as biz_line_cd,
   'Corporate Travel' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '7969632b-0e74-420a-a605-85bcd68e957a' as biz_line_adw_key ,
   'CTC' as biz_line_cd,
   'Contact Centers' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select 'c467d752-96d1-45a0-9f80-2ce877dbf25d' as biz_line_adw_key ,
   'Discount' as biz_line_cd,
   'Discounts' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }}

   UNION ALL

   select 'c0aace40-626b-4c8e-bffe-97155baafd92' as biz_line_adw_key ,
   'DRV' as biz_line_cd,
   'Driver Services' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '52913c1a-bd2c-4fe8-bb83-7a37c19017fb' as biz_line_adw_key ,
   'FIN' as biz_line_cd,
   'Finance' as biz_line_desc,
   CURRENT_DATETIME as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '1627c4ca-300d-45b1-bdb5-72c91dd2ec33' as biz_line_adw_key ,
   'FND' as biz_line_cd,
   'Traffic Safety' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '87b3a51b-9344-423f-a78f-86dc92f489b3' as biz_line_adw_key ,
   'Found' as biz_line_cd,
   'Foundation' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '125715f1-9ded-4683-a2bb-77d82f1a53fc' as biz_line_adw_key ,
   'FS' as biz_line_cd,
   'Financial Services' as biz_line_desc,
   CURRENT_DATETIME as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '0e6cfdcd-ebbf-475c-a93a-1e1c405a3996' as biz_line_adw_key ,
   'FSD' as biz_line_cd,
   'Financial Services & Discounts' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '90be2f81-b0a5-4e14-bd15-7c7761c5ca7c' as biz_line_adw_key ,
   'HR' as biz_line_cd,
   'Human Resources' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '49e503c9-8f79-4574-8391-68bfc6ca8f5d' as biz_line_adw_key ,
   'INS' as biz_line_cd,
   'Insurance' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select 'aa24f481-84b0-4b1d-b847-7719198d054b' as biz_line_adw_key ,
   'IT' as biz_line_cd,
   'Information Technology' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '1ee7aba2-9f5f-432d-8d62-f69f15e0b5c5' as biz_line_adw_key ,
   'LGL' as biz_line_cd,
  'Legal' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '400f9b55-affc-4daf-9f1c-e2304f5b1525' as biz_line_adw_key ,
   'LT' as biz_line_cd,
   'Leisure Travel' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }}  as integrate_insert_batch_number

   UNION ALL

   select '33b05324-9a68-4775-9c89-eee81867b23d' as biz_line_adw_key ,
   'MBR' as biz_line_cd,
   'Membership' as biz_line_desc,
   CURRENT_DATETIME as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select 'f80097f9-d56a-4451-a0a0-0a2c421a97f7' as biz_line_adw_key ,
   'MKT' as biz_line_cd,
   'Marketing' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '56aa2b0d-c4d5-4d88-90ea-dfeb62c5398a' as biz_line_adw_key ,
   'OPR' as biz_line_cd,
   'Operations Support Admin' as biz_line_desc,
   CURRENT_DATETIME as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select 'c3940fa2-8d43-4027-aa6c-71ffe000823a' as biz_line_adw_key ,
   'PGA' as biz_line_cd,
   'Public Relations & Government Affairs' as biz_line_desc,
   CURRENT_DATETIME as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '01035672-d6ee-4ffe-86c0-51aae2447294' as biz_line_adw_key ,
   'PL' as biz_line_cd,
   'Personal Lines Insurance' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '87e0ce35-34c5-4d19-8339-830daf5ba6af' as biz_line_adw_key ,
   'RTL' as biz_line_cd,
   'Retail Operations' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '869f187f-5222-4a41-86a2-c56c1eccbf1f' as biz_line_adw_key ,
   'TRV' as biz_line_cd,
   'Travel' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number

   UNION ALL

   select '07b2d2b4-99b2-4f6a-8a0c-f861364bf013' as biz_line_adw_key ,
   'WRL' as biz_line_cd,
   'RAAA World' as biz_line_desc,
   CURRENT_DATETIME  as integrate_insert_datetime,
   {{ dag_run.id }} as integrate_insert_batch_number
  ) a
 where 0 = (select count(biz_line_adw_key)
              from `{{var.value.INTEGRATION_PROJECT}}.adw.dim_business_line`);



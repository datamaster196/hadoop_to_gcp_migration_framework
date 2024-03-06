CREATE OR REPLACE FUNCTION `{{ var.value.INTEGRATION_PROJECT }}.udfs.parse_zipcode`(postal_code STRING, postal4_code string, country string, type STRING) AS 
    (

    case when lower(type) = 'prefix' then
      case when country='CAN' then substr(postal_code,1,7)
	       when length(postal_code)>=5
				and safe_cast(substr(postal_code,1,5) as int64) is not null
				then substr(postal_code,1,5)
			else null
			end
    when lower(type) = 'suffix' then
      case when country='CAN' then null
           when length(trim(postal4_code))>1 and safe_cast(postal4_code as int64) is not null 
                then postal4_code
	       when length(postal_code)=9 and safe_cast(substr(postal_code,6,4) as int64) is not null
		        then substr(postal_code,6,4)
		   when length(postal_code)=10 and safe_cast(substr(postal_code,7,4) as int64) is not null
		        then substr(postal_code,7,4)
			else null 
			end
     else null 
	 end
    )
    ;
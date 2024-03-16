select name
       ,code as exchange_cd
       ,country 
       ,currency
       ,countryISO2 
       ,countryISO3
       ,JSON_ARRAYAGG(
            JSON_OBJECT(
                    'operatingMic1', SUBSTRING_INDEX(`OperatingMIC`,', ',1),
                    'operatingMic2', SUBSTRING_INDEX(`OperatingMIC`,', ',-1)
        )) as operatingMIC
        ,CURRENT_TIMESTAMP as updated_at
 from {{ ref('stg_exchange') }}
 group by
 name
,code
,country 
,currency
,countryISO2 
,countryISO3
 
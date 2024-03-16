select  CONCAT_WS('-',exchange_cd,CURRENT_TIMESTAMP()) as exchange_id
       ,name
       ,exchange_cd
       ,country 
       ,currency
       ,countryISO2 
       ,countryISO3
       ,operatingMIC
       ,updated_at
       ,dbt_scd_id
       ,dbt_updated_at
       ,dbt_valid_from
       ,dbt_valid_to
from {{ ref('snp_exchange') }}
where dbt_valid_to is null
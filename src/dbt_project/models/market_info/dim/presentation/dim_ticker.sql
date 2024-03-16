select  CONCAT_WS('-',ticker_cd,CURRENT_TIMESTAMP()) as ticker_id
       ,ticker_cd
       ,ticker_name
       ,country 
       ,exchange
       ,currency
       ,type
       ,Isin
       ,updated_at
       ,dbt_scd_id
       ,dbt_updated_at
       ,dbt_valid_from
       ,dbt_valid_to
from {{ ref('snp_ticker') }}
where dbt_valid_to is null
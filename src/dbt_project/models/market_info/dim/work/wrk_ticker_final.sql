select code as ticker_cd
      ,name as ticker_name
      ,country
      ,exchange 
      ,currency
      ,type
      ,Isin
      ,exchange_cd 
     ,CURRENT_TIMESTAMP as updated_at
 from {{ ref('stg_ticker') }}

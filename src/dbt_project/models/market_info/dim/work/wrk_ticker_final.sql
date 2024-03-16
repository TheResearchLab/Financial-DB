select code as ticker_cd
      ,name as ticker_name
      ,country
      ,exchange 
      ,currency
      ,type
      ,Isin 
     ,CURRENT_TIMESTAMP as updated_at
 from {{ ref('stg_ticker') }}

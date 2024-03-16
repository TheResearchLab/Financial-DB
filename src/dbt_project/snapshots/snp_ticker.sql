{% snapshot snp_ticker %}

{{
    config(
      target_schema='the-research-lab-db',
      unique_key='ticker_cd',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * 
from {{ ref('wrk_ticker_final') }}

{% endsnapshot %}
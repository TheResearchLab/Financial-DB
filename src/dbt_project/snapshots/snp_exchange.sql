{% snapshot snp_exchange %}

{{
    config(
      target_schema='the-research-lab-db',
      unique_key='exchange_cd',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * 
from {{ ref('wrk_exchange_final') }}

{% endsnapshot %}
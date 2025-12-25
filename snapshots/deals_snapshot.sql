{% snapshot deals_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='deal_id',
      strategy='check',     
      check_cols=['deal_stage', 'estimated_value', 'probability_percent']
    )
}}

select * from {{ ref('src_deals') }}

{% endsnapshot %}
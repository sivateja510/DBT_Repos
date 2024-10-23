{% snapshot scd_fct_sellers %}
{{
  config(
    target_schema='dev',
    unique_key='id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True,
    merge_columns=['surrogate_key']
  )
}}
SELECT * 
FROM {{ref('fct_sellers')}}

{% endsnapshot %}
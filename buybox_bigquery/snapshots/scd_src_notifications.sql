{% snapshot scd_src_notification %}
{{
  config(
    target_schema='dev',
    unique_key='surrogatekey',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True,
    merge_columns=['surrogatekey']
  )
}}
SELECT * ,current_timestamp() as updated_at
FROM {{ref('src_notification')}}

{% endsnapshot %}
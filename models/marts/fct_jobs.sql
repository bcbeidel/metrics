{{ 
  config(
    materialized= 'incremental', 
    unique_key='job_id',
    partition_by={
      "field": "created_at",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    clustered_by=['query_hash', 'user_name']
  ) 
}}

with job_details as (
  {% if target.type == 'redshift' %} 
    select * from {{ ref('stg_jobs_redshift') }} 
  {% endif %}
  {% if target.type == 'bigquery' %} 
    select * from {{ ref('stg_jobs_bigquery') }} 
  {% endif %}
)

select 
  *
, case when left(lower(query_text), 200) LIKE '%select%' then 1 else 0 end as is_select_statement
from job_details

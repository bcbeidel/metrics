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
    select * from {{ ref('stg_job_details_redshift') }} 
  {% endif %}
  {% if target.type == 'bigquery' %} 
    select * from {{ ref('stg_job_details_bigquery') }} 
  {% endif %}
)

select * from job_details

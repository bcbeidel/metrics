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
),

user_status as (
  select
    user_name
  , is_user_account
  , user_team
  from {{ ref('fct_users') }}
)

select
  job_details.*
, user_status.is_user_account as is_user_account
, user_status.user_team as user_team
from job_details
left join user_status
  on job_details.user_name = user_status.user_name

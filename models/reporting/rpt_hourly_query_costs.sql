{{ config(materialized='table') }}

with base as (

  select * from {{ ref('fct_jobs') }}

),

hourly as (

  select
  {% if target.type == 'redshift' %} 
    date_trunc('hour', created_at)    as query_hour
  {% endif %}
  {% if target.type == 'bigquery' %} 
    datetime_trunc(created_at, hour)  as query_hour
  {% endif %}
  , sum(estimated_cost_usd)           as estimated_cost_usd 
  from base
  group by 1, 2
  order by 2 desc

)

select
    *
  , {{ dbt_utils.surrogate_key(['query_hour']) }} as row_id
from hourly
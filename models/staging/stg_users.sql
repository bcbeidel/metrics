{{ config(materialized='ephemeral') }}

select

{% if target.type == 'redshift' %} 
  cast(user_id as varchar)          as user_id
, cast(user_name as varchar)        as user_name
, cast(is_user_account as varchar)  as is_user_account
{% endif %}

{% if target.type == 'bigquery' %} 
  cast(user_id as string)          as user_id
, cast(user_name as string)        as user_name
, cast(is_user_account as string)  as is_user_account
{% endif %}

from {{ var('metrics__user_mapping_table') }} as 

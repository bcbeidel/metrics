{{ config(materialized='ephemeral') }}

with users as (
  select 
    regexp_replace(usename, '[^[:alnum:][:blank:][:punct:]]', '') as user_name, 
    usesysid                                                      as user_id
  from pg_user
),

transformed as (

  select
    STL_QUERY.query                                                                   as job_id
  , STL_QUERY.starttime                                                               as created_at
  , STL_QUERY.starttime                                                               as started_at
  , STL_QUERY.endtime                                                                 as ended_at
  , 'QUERY'                                                                           as statement_type
  , regexp_replace(STL_QUERY.querytxt, '[^[:alnum:][:blank:][:punct:]]', '')          as query_text
  , users.user_name                                                                   as user_id
  , users.user_id                                                                     as user_name
  , CASE WHEN STL_QUERY.aborted = 1 THEN 'aborted' else 'complete' end                as query_status
  , null                                                                              as query_cost
  , DATEDIFF(milliseconds, STL_QUERY.starttime, STL_QUERY.endtime)  * 1.0 / 1000.0    as query_duration_seconds
  , null                                                                              as error_status
  , null                                                                              as error_code
  -- Redshift Specific Columns
  , STL_QUERY.query                                                                   as query_id
  , STL_QUERY.xid                                                                     as transaction_id
  , STL_QUERY.pid                                                                     as process_id
  , STL_QUERY.label                                                                   as label

  from STL_QUERY
  left join users on STL_QUERY.userid = users.user_id
  where STL_QUERY.starttime >= DATEADD(DAY, -{{ var('metrics__days_of_history') }}, GETDATE())

  {% if is_incremental() %}
    and STL_QUERY.starttime >= (select DATEADD(max(STL_QUERY.starttime), INTERVAL -1 HOUR) from {{ this }} )
  {% endif %}
  {% if target.name != 'prod' %}
    and STL_QUERY.starttime >= DATEADD(DAY, -7, GETDATE())
  {% endif %}

)

select 
  *
, {{ dbt_utils.surrogate_key(['query_text']) }} as query_text_md5
from transformed
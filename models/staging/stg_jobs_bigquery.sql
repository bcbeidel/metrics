{{ config(materialized='ephemeral') }}

select
  cast(job_id as string)	                                          as job_id
, cast(creation_time as timestamp)	                                as created_at
, cast(start_time as timestamp)                                     as started_at
, cast(end_time as timestamp)                                       as ended_at	
, cast(statement_type  as string)	                                  as statement_type
, {{ dbt_utils.surrogate_key(['query']) }}	                        as query_text_md5
, cast(query  as string)		                                        as query_text
, cast(user_email as string)                                        as user_id
, cast(user_email as string)	                                      as user_name
, cast(state as string)		                                          as query_status
, round(
  safe_multiply(
    5
  , safe_divide(
      cast(total_bytes_billed as decimal)
    , POWER(10, 12)) 
  ), 2
)                                                                   as estimated_cost_usd
, (timestamp_diff(end_time, start_time, MILLISECOND) * 1.0 / 1000)  as query_duration_seconds
, cast(error_result.reason as string)	                              as error_code
, cast(error_result.message as string)                              as error_message
-- provide boolean logic to identify select statements
, case when trim(lower(statement_type)) = 'select'
       then true
       else false
  end                                                               as is_select_statement

from `region-us`.`INFORMATION_SCHEMA`.`JOBS_BY_PROJECT`
where creation_time >= (DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -{{ var('metrics__days_of_history') }} DAY))

{% if is_incremental() %}
  and creation_time >= (select TIMESTAMP_ADD(max(created_at), INTERVAL -1 HOUR) from {{ this }} )
{% endif %}
{% if target.name != 'prod' %}
  and creation_time >= (DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -7 DAY))
{% endif %}
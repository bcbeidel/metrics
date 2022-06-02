{{ config(materialized='ephemeral') }}

select
  job_id	                                                                as job_id
, creation_time	                                                          as created_at
, start_time                                                              as started_at
, end_time                                                                as ended_at	
, statement_type                                                          as statement_type
, {{ dbt_utils.surrogate_key(['query']) }}	                              as query_text_md5
, query	                                                                  as query_text
, user_email                                                              as user_id
, user_email	                                                            as user_name
, state		                                                                as query_status
, round(
  safe_multiply(
    5
  , safe_divide(
      total_bytes_billed
    , POWER(10, 12)) 
  ), 2
)                                                                         as query_cost
, (timestamp_diff(end_time, start_time, MILLISECOND) * 1.0 / 1000)        as query_duration_seconds
, error_result.reason	                                                    as error_code
, error_result.message                                                    as error_message
from `region-us`.`INFORMATION_SCHEMA`.`JOBS_BY_PROJECT`
where creation_time >= (DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -{{ var('wh_metrics__days_of_history') }} DAY))

{% if is_incremental() %}
  and creation_time >= (select TIMESTAMP_ADD(max(created_at), INTERVAL -1 HOUR) from {{ this }} )
{% endif %}
{% if target.name != 'prod' %}
  and creation_time >= (DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -7 DAY))
{% endif %}
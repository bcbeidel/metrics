with base as (

  select
    created_at_date
  , case when p95_query_duration_seconds < p95_query_duration_seconds_SLO then 1 else 0 end as is_acheived_slo_p95_query_speed
  , case when p99_query_duration_seconds < p99_query_duration_seconds_SLO then 1 else 0 end as is_acheived_slo_p99_query_speed
  from {{ ref('rpt_daily_metrics') }}
  order by created_at_date desc

)

select * from base
with all_jobs as (

  select
    jobs.job_id                                   as job_id
  , cast(jobs.created_at as date)                 as created_at_date
  , jobs.query_text_md5                           as query_text_md5
  , jobs.query_text                               as query_text
  , cast(jobs.query_duration_seconds as decimal)  as query_duration_seconds
  , jobs.user_name                                as user_name
  , users.is_user_account                         as is_user_account
  , jobs.is_select_statement                      as is_select_statement
  , cast(jobs.estimated_cost_usd as decimal)      as estimated_cost_usd
  from {{ ref('fct_jobs') }} as jobs
  left join {{ ref ('dim_users') }} as users
    on jobs.user_name = users.user_id

),

user_queries as (

  select * from all_jobs
  where is_user_account is true 
    and is_select_statement is true

),

date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=dbt_date.n_days_ago(var('metrics__days_of_history')),
        end_date=dbt_date.today()
    )}}
    
),

date_spine_renamed as (

    select 
        cast(date_day as date) as created_at_date
    from date_spine

),

daily_costs as (

    select
      created_at_date                                                                               as created_at_date
    , sum(case when is_user_account is true then 1 else 0 end)                                      as job_count_users
    , sum(case when is_user_account is false then 1 else 0 end)                                     as job_count_nonusers
    , count(job_id)                                                                                 as job_count_all
    , round(sum(case when is_user_account is true then estimated_cost_usd else 0.0 end), 2)         as estimated_cost_usd_by_users
    , round(sum(case when is_user_account is false then estimated_cost_usd else 0.0 end), 2)        as estimated_cost_usd_by_nonusers
    , round(sum(case when estimated_cost_usd is not null then estimated_cost_usd else 0.0 end), 2)  as estimated_cost_usd_all  
    from all_jobs
    group by 1
    order by 1 desc

),

{% if target.type == 'bigquery' %} 
  
user_query_duration_percentiles_approximated as (

  select
    created_at_date                               as created_at_date
  , count(job_id)                                 as user_query_count
  , approx_quantiles(query_duration_seconds, 100) as query_duration_percentiles
  from user_queries
  group by 1
  
),

user_query_duration_percentiles as (

  select 
    created_at_date                             as created_at_date
  , user_query_count                            as user_query_count
  , query_duration_percentiles[safe_offset(95)] as p95_query_duration_seconds
  , query_duration_percentiles[safe_offset(99)] as p99_query_duration_seconds
  from user_query_duration_percentiles_approximated

)

{% endif %}
{% if target.type == 'redshift' %} 

user_query_duration_percentiles as (

  select distinct
    created_at_date                                                                                              as created_at_date
  , percentile_cont(0.95) within group (order by query_duration_seconds asc) over (partition by created_at_date) as p95_query_duration_seconds
  , percentile_cont(0.99) within group (order by query_duration_seconds asc) over (partition by created_at_date) as p99_query_duration_seconds
  from user_queries

)

{% endif %}

select 
  date_spine_renamed.created_at_date                          as created_at_date
, daily_costs.job_count_users                                 as job_count_users
, daily_costs.job_count_nonusers                              as job_count_nonusers
, daily_costs.job_count_all                                   as job_count_all
, daily_costs.estimated_cost_usd_by_users                     as estimated_cost_usd_by_users
, daily_costs.estimated_cost_usd_by_nonusers                  as estimated_cost_usd_by_nonusers
, daily_costs.estimated_cost_usd_all                          as estimated_cost_usd_all
, user_query_duration_percentiles.p95_query_duration_seconds  as p95_query_duration_seconds
, user_query_duration_percentiles.p99_query_duration_seconds  as p99_query_duration_seconds
, {{ var('metrics__p95_query_duration_seconds_SLO') }}        as p95_query_duration_seconds_SLO
, {{ var('metrics__p99_query_duration_seconds_SLO') }}        as p99_query_duration_seconds_SLO

from date_spine_renamed
left join daily_costs
       on date_spine_renamed.created_at_date = daily_costs.created_at_date
left join user_query_duration_percentiles
       on date_spine_renamed.created_at_date = user_query_duration_percentiles.created_at_date
order by 1 desc

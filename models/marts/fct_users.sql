with base as (
  select
    case
      when trim(lower(is_user_account)) in ('0', 'false') then 0
      when trim(lower(is_user_account)) in ('1', 'true') then 1
      else null
    end as is_user_account,
    {{ dbt_utils.star(from=ref('stg_users'), except=["is_user_account"]) }}
  from {{ ref('stg_users') }}
)

select * from base

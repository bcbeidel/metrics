with base as (
  select
    case
      when trim(lower(is_user_account)) in ('0', 'false', 'no') then false
      when trim(lower(is_user_account)) in ('1', 'true', 'yes') then true
      else null
    end as is_user_account,
    {{ dbt_utils.star(from=ref('stg_users'), except=["is_user_account"]) }}
  from {{ ref('stg_users') }}
)

select * from base

with base as (

  select 
    * except(is_user_account)
  , trim(lower(safe_cast(is_user_account as string))) as is_user_account
  from {{ var('metrics__user_mapping_table') }}

)

select
  * except(is_user_account)
, case
    when is_user_account in ('0', 'false') then 0
    when is_user_account in ('1', 'true') then 1
    else null
  end as is_user_account
from base

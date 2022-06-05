
select
  user_id
, user_name
, case
    when trim(lower(is_user_account)) in ('0', 'false', 'no') then false
    when trim(lower(is_user_account)) in ('1', 'true', 'yes') then true
    else null
  end as is_user_account
from {{ ref('stg_users') }}

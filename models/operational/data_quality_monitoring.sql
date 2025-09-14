select 
    'installs' as data_source,
    count(*) as record_count,
    count(distinct justplay_user_id) as unique_users,
    sum(case when adjust_user_id is null then 1 else 0 end) as missing_adjust_ids,
    current_timestamp as profiled_at
from {{ ref('int_installs') }}
union all
select 
    'revenue' as data_source,
    count(*) as record_count,
    count(distinct justplay_user_id) as unique_users,
    sum(case when revenue_usd <= 0 then 1 else 0 end) as invalid_revenue,
    current_timestamp as profiled_at
from {{ ref('int_revenue') }}
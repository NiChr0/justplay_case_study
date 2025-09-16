select
    installed_at::date as install_date,
    acquisition_channel,
    campaign_name,
    count(*) as daily_installs,
    count(distinct justplay_user_id) as unique_users,
    count(*) - count(distinct justplay_user_id) as duplicate_installs

from {{ ref('int_installs') }}
group by 1, 2, 3
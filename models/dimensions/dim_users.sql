-- models/marts/users/dim_users.sql
{{
  config(
    materialized = 'table'
    )
}}

with user_installs_ranked as (
    select 
        justplay_user_id,
        installed_at,
        country_code,
        os_name,
        acquisition_channel,
        campaign_name,
        row_number() over (partition by justplay_user_id order by installed_at asc) as install_rank,
        row_number() over (partition by justplay_user_id order by installed_at desc) as install_rank_desc
    from {{ ref('int_installs') }}
),

first_install as (
    select 
        justplay_user_id,
        installed_at as first_install_at,
        installed_at::date as first_cohort_date,
        country_code as first_country_code,
        os_name as first_os_name,
        acquisition_channel as first_acquisition_channel,
        campaign_name as first_campaign_name
    from user_installs_ranked
    where install_rank = 1
),

latest_install as (
    select 
        justplay_user_id,
        installed_at as latest_install_at,
        installed_at::date as latest_cohort_date,
        country_code as latest_country_code,
        os_name as latest_os_name,
        acquisition_channel as latest_acquisition_channel,
        campaign_name as latest_campaign_name
    from user_installs_ranked
    where install_rank_desc = 1
),

user_revenue as (
    select 
        justplay_user_id,
        sum(revenue_usd) as lifetime_revenue,
        count(distinct created_at::date) as active_days,
        max(created_at) as last_revenue_at
    from {{ ref('int_revenue') }}
    group by 1
)

select 
    fi.justplay_user_id,
    fi.first_cohort_date,
    fi.first_country_code,
    fi.first_os_name,
    fi.first_acquisition_channel,
    fi.first_campaign_name,
    li.latest_cohort_date,
    li.latest_country_code,
    li.latest_os_name,
    li.latest_acquisition_channel,
    li.latest_campaign_name,
    coalesce(ur.lifetime_revenue, 0) as ltv,
    coalesce(ur.active_days, 0) as active_days,
    case when ur.lifetime_revenue > 0 then 1 else 0 end as is_payer,
    datediff('day', fi.first_install_at, current_date) as days_since_first_install,
    datediff('day', li.latest_install_at, current_date) as days_since_latest_install,
    case when fi.first_cohort_date != li.latest_cohort_date then 1 else 0 end as is_reinstaller
from first_install fi
left join latest_install li on fi.justplay_user_id = li.justplay_user_id
left join user_revenue ur on fi.justplay_user_id = ur.justplay_user_id
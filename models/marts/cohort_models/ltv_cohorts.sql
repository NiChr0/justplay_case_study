
{{
  config(
    materialized = 'table'
  )
}}

with user_installs as (
    select 
        justplay_user_id,
        min(installed_at) as first_install_at,
        min(installed_at::date) as cohort_date,
        max(country_code) as install_country,
        max(os_name) as install_platform,
        max(acquisition_channel) as install_channel,
        max(campaign_name) as install_campaign
    from {{ ref('int_installs') }}
    {{dbt_utils.group_by(n=1)}}
),

user_revenue_with_days as (
    select 
        r.justplay_user_id,
        r.created_at,
        r.revenue_usd,
        i.first_install_at,
        i.cohort_date,
        i.install_country,
        i.install_platform, 
        i.install_channel,
        i.install_campaign,
        datediff('day', i.first_install_at, r.created_at) as days_since_install
    from {{ ref('int_revenue') }} r
    inner join user_installs i on r.justplay_user_id = i.justplay_user_id
),

cohort_ltv_calc as (
    select 
        justplay_user_id,
        cohort_date,
        install_country,
        install_platform,
        install_channel,
        install_campaign,
        
        -- LTV by time windows
        sum(case when days_since_install = 0 then revenue_usd else 0 end) as ltv_day_0,
        sum(case when days_since_install <= 7 then revenue_usd else 0 end) as ltv_day_7,
        sum(case when days_since_install <= 14 then revenue_usd else 0 end) as ltv_day_14,
        sum(case when days_since_install <= 30 then revenue_usd else 0 end) as ltv_day_30,
        
        -- Additional metrics
        max(days_since_install) as last_active_day,
        count(distinct case when days_since_install <= 7 then created_at::date end) as active_days_week_1
        
    from user_revenue_with_days
    {{dbt_utils.group_by(n=6)}}
),

-- Include users with zero revenue
final_cohort as (
    select 
        i.justplay_user_id,
        i.cohort_date,
        i.install_country,
        i.install_platform,
        i.install_channel,
        i.install_campaign,
        
        coalesce(c.ltv_day_0, 0) as ltv_day_0,
        coalesce(c.ltv_day_7, 0) as ltv_day_7,
        coalesce(c.ltv_day_14, 0) as ltv_day_14,
        coalesce(c.ltv_day_30, 0) as ltv_day_30,
        
        coalesce(c.last_active_day, 0) as last_active_day,
        coalesce(c.active_days_week_1, 0) as active_days_week_1,
        
        case when c.justplay_user_id is not null then true else false end as is_payer
        
    from user_installs i
    left join cohort_ltv_calc c on i.justplay_user_id = c.justplay_user_id
)

select * from final_cohort
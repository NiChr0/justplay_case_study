{{
  config(
    materialized = 'table'
    )
}}

with installs_base as (
    select *
    from {{ ref('int_installs') }}
),

revenue_base as (
    select *
    from {{ ref('int_revenue') }}
),

user_install_attribution as (
    select
        justplay_user_id,
        min(installed_at) as first_install_date,
        max(country_code) as install_country,
        max(os_name) as install_platform,
        max(acquisition_channel) as install_channel
    from installs_base
    group by 1
),

user_revenue_summary as (
    select
        justplay_user_id,
        sum(revenue_usd) as total_revenue,
        count(*) as revenue_events,
        min(created_at) as first_revenue_date,
        max(created_at) as last_revenue_date
    from revenue_base
    group by 1
),

daily_aggregation as (
    select
        ui.first_install_date::date as install_date,
        ui.install_country,
        ui.install_platform,
        ui.install_channel,
        
        count(distinct ui.justplay_user_id) as total_users_installed,
        count(distinct ur.justplay_user_id) as users_with_revenue,
        sum(coalesce(ur.total_revenue, 0)) as total_revenue,
        
        case 
            when count(distinct ui.justplay_user_id) > 0 
            then sum(coalesce(ur.total_revenue, 0)) / count(distinct ui.justplay_user_id) 
            else 0 
        end as arpu,
        
        case 
            when count(distinct ui.justplay_user_id) > 0 
            then count(distinct ur.justplay_user_id)::float / count(distinct ui.justplay_user_id)::float 
            else 0 
        end as monetization_rate
        
    from user_install_attribution ui
    left join user_revenue_summary ur on ui.justplay_user_id = ur.justplay_user_id
    {{dbt_utils.group_by(n=4)}}
)

select * from daily_aggregation
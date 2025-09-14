{{
  config(
    materialized = 'table'
    )
}}

with ad_spend_base as (
    select *
    from {{ ref('int_ad_spend') }}
),

revenue_base as (
    select *
    from {{ ref('int_revenue') }}
),

installs_base as (
    select *
    from {{ ref('int_installs') }}
),

-- Get revenue attributed to channels via user joins
channel_revenue as (
    select
        i.installed_at::date as install_date,
        i.country_code,
        i.os_name,
        i.acquisition_channel,
        i.campaign_name,
        i.campaign_creative,
        sum(r.revenue_usd) as total_revenue
    from installs_base i
    inner join revenue_base r on i.justplay_user_id = r.justplay_user_id
    group by 1, 2, 3, 4, 5, 6
),

channel_performance as (
    select
        ads.report_date,
        ads.country_code,
        ads.os_name,
        ads.acquisition_channel,
        ads.campaign_name,
        ads.campaign_creative,
        
        -- Aggregated metrics
        sum(ads.ad_spend_usd) as total_ad_spend,
        sum(ads.network_clicks) as total_network_clicks,
        sum(ads.network_impressions) as total_network_impressions,
        sum(ads.network_installs) as total_network_installs,
        sum(ads.adjust_installs) as total_adjust_installs,
        sum(coalesce(rev.total_revenue, 0)) as total_revenue,
        
        -- Calculated metrics
        case 
            when sum(ads.network_impressions) > 0 
            then sum(ads.network_clicks)::float / sum(ads.network_impressions)::float 
            else null 
        end as ctr,
        
        case 
            when sum(ads.network_clicks) > 0 
            then sum(ads.adjust_installs)::float / sum(ads.network_clicks)::float 
            else null 
        end as install_conversion_rate,
        
        case 
            when sum(ads.network_installs) > 0 
            then (sum(ads.network_installs) - sum(ads.adjust_installs))::float / sum(ads.network_installs)::float 
            else null 
        end as attribution_discrepancy_rate,
        
        case 
            when sum(ads.adjust_installs) > 0 
            then sum(ads.ad_spend_usd) / sum(ads.adjust_installs) 
            else null 
        end as cost_per_install,
        
        case 
            when sum(ads.adjust_installs) > 0 
            then sum(coalesce(rev.total_revenue, 0)) / sum(ads.adjust_installs) 
            else null 
        end as revenue_per_install,
        
        case 
            when sum(ads.network_impressions) > 0 
            then sum(coalesce(rev.total_revenue, 0)) / sum(ads.network_impressions) 
            else null 
        end as revenue_per_impression,
        
        case 
            when sum(ads.network_impressions) > 0 
            then sum(ads.ad_spend_usd) / sum(ads.network_impressions) * 1000 
            else null 
        end as cpm
        
    from ad_spend_base ads
    left join channel_revenue rev 
        on ads.report_date = rev.install_date
        and ads.country_code = rev.country_code
        and ads.os_name = rev.os_name
        and ads.acquisition_channel = rev.acquisition_channel
        and ads.campaign_name = rev.campaign_name
        and ads.campaign_creative = rev.campaign_creative
    {{dbt_utils.group_by(n=6)}}
)

select * from channel_performance
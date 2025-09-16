
{{
  config(
    materialized = 'table'
  )
}}

with cohort_metrics as (
    select 
        cohort_date,
        install_country,
        install_platform,
        install_channel,
        install_campaign,
        
        count(*) as total_users,
        count(case when is_payer = 1 then 1 end) as paying_users,
        
        -- Average LTV by time window
        avg(ltv_day_0) as avg_ltv_day_0,
        avg(ltv_day_7) as avg_ltv_day_7,
        avg(ltv_day_14) as avg_ltv_day_14,
        avg(ltv_day_30) as avg_ltv_day_30,
        
        -- Monetization rates
        count(case when ltv_day_0 > 0 then 1 end)::float / count(*)::float as monetization_rate_day_0,
        count(case when ltv_day_7 > 0 then 1 end)::float / count(*)::float as monetization_rate_day_7,
        count(case when ltv_day_14 > 0 then 1 end)::float / count(*)::float as monetization_rate_day_14,
        count(case when ltv_day_30 > 0 then 1 end)::float / count(*)::float as monetization_rate_day_30
        
    from {{ ref('ltv_cohorts') }}
    {{dbt_utils.group_by(n=5)}}
),

-- Get ad spend data aggregated
ad_spend_summary as (
    select 
        report_date,
        country_code,
        os_name,
        acquisition_channel,
        campaign_name,
        sum(total_ad_spend) as total_ad_spend,
        sum(total_adjust_installs) as total_adjust_installs
    from {{ ref('performance__kpis') }}
    {{dbt_utils.group_by(n=5)}}
),

-- Join cohort metrics with ad spend
cohort_with_spend as (
    select 
        c.*,
        coalesce(ads.total_ad_spend, 0) as total_ad_spend,
        coalesce(ads.total_adjust_installs, c.total_users) as attributed_installs,
        
        -- ROAS calculations
        case when ads.total_ad_spend > 0 then (c.avg_ltv_day_7 * c.total_users) / ads.total_ad_spend else null end as roas_day_7,
        case when ads.total_ad_spend > 0 then (c.avg_ltv_day_14 * c.total_users) / ads.total_ad_spend else null end as roas_day_14,
        case when ads.total_ad_spend > 0 then (c.avg_ltv_day_30 * c.total_users) / ads.total_ad_spend else null end as roas_day_30,
        
        -- CPI
        case when ads.total_adjust_installs > 0 then ads.total_ad_spend / ads.total_adjust_installs else null end as cpi
        
    from cohort_metrics c
    left join ad_spend_summary ads 
        on c.cohort_date = ads.report_date
        and c.install_country = ads.country_code
        and c.install_platform = ads.os_name
        and c.install_channel = ads.acquisition_channel
        and c.install_campaign = ads.campaign_name
)

select * from cohort_with_spend
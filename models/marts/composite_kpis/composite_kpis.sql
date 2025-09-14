{{
  config(
    materialized = 'table'
    )
}}

with channel_performance as (
    select *
    from {{ ref('performance__kpis') }}
),

user_revenue as (
    select *
    from {{ ref('users__kpis') }}
),

combined_metrics as (
    select
        cp.report_date,
        cp.country_code,
        cp.os_name,
        cp.acquisition_channel,
        cp.campaign_name,
        cp.campaign_creative,
        
        -- Base metrics from channel performance
        cp.total_ad_spend,
        cp.total_adjust_installs,
        cp.total_revenue,
        cp.ctr,
        cp.install_conversion_rate,
        cp.attribution_discrepancy_rate,
        cp.cost_per_install,
        cp.revenue_per_install,
        cp.revenue_per_impression,
        cp.cpm,
        
        -- Base metrics from user revenue
        ur.total_users_installed,
        ur.users_with_revenue,
        ur.arpu,
        ur.monetization_rate,
        
        -- Composite KPIs
        case 
            when cp.cost_per_install > 0 and cp.revenue_per_install is not null and ur.monetization_rate is not null
            then (cp.revenue_per_install * ur.monetization_rate) / cp.cost_per_install 
            else null 
        end as marketing_efficiency_score,
        
        case 
            when cp.revenue_per_install is not null and cp.install_conversion_rate is not null and cp.attribution_discrepancy_rate is not null
            then cp.revenue_per_install * cp.install_conversion_rate * (1 - cp.attribution_discrepancy_rate) 
            else null 
        end as channel_quality_index,
        
        case 
            when cp.total_adjust_installs > 0 and ur.arpu is not null
            then (ur.users_with_revenue::float / cp.total_adjust_installs::float) * ur.arpu 
            else null 
        end as user_value_realization_rate,
        
        case 
            when cp.revenue_per_impression is not null and cp.ctr is not null and cp.cpm > 0
            then (cp.revenue_per_impression * cp.ctr) / (cp.cpm / 1000) 
            else null 
        end as media_roi,
        
        case 
            when cp.revenue_per_install is not null and ur.monetization_rate is not null
            then cp.revenue_per_install * ur.monetization_rate 
            else null 
        end as cohort_performance_score
        
    from channel_performance cp
    left join user_revenue ur 
        on cp.report_date = ur.install_date
        and cp.country_code = ur.install_country
        and cp.os_name = ur.install_platform
        and cp.acquisition_channel = ur.install_channel
)

select * from combined_metrics
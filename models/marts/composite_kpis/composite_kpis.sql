-- models/marts/composite_kpis/composite_kpis_simplified.sql
{{
  config(
    materialized = 'table'
    )
}}

with base_metrics as (
    select * from {{ ref('performance__kpis') }}
)

select 
    *,
    total_revenue / nullif(total_ad_spend, 0) as roas,
    
    case 
        when total_revenue / nullif(total_ad_spend, 0) >= 1.0 then 'highly_profitable'
        when total_revenue / nullif(total_ad_spend, 0) >= 0.5 then 'profitable'
        when total_revenue / nullif(total_ad_spend, 0) >= 0.2 then 'barely_profitable'
        else 'unprofitable'
    end as channel_profitability_status,
    
    case
        when ctr > 0.02 and attribution_discrepancy_rate < 0.1 then 'high'
        when ctr > 0.01 and attribution_discrepancy_rate < 0.2 then 'medium'
        else 'low'
    end as traffic_quality

from base_metrics
with source as(
    select *
    from {{ ref('ad_spent_installs_raw') }}
)


select
    report_day::date as report_date,
    trim(lower(country_code)) as country_code,
    trim(lower(os_name)) as os_name,
    trim(lower(channel)) as acquisition_channel,
    trim(lower(campaign)) as campaign_name,
    trim(lower(creative)) as campaign_creative,
    coalesce(cost,0) as ad_spend_usd,
    coalesce(network_clicks,0) as network_clicks,
    coalesce(network_impressions,0) as network_impressions,
    coalesce(network_installs,0) as network_installs,
    coalesce(installs,0) as adjust_installs
    --coalesce(network_installs_diff,0) as network_installs_diff
from 
    source
{{ dbt_utils.group_by(n=11) }}
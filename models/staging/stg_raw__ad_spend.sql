with source as(
    select *
    from {{ ref('ad_spent_installs_raw') }}
),

renamed as(
select
    report_day::date as report_date,
    trim(lower(country_code)) as country_code,
    trim(lower(os_name)) as os_name,
    trim(lower(channel)) as acquisition_channel,
    trim(lower(campaign)) as campaign_name,
    trim(lower(creative)) as campaign_creative,
    cost as ad_spend_usd,
    network_clicks as network_clicks,
    network_impressions as network_impressions,
    network_installs as network_installs,
    installs as adjust_installs
    --coalesce(network_installs_diff,0) as network_installs_diff
from 
    source
)

select * 
from renamed
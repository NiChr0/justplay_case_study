with source as(
    select *
    from {{ ref('installs_raw') }}
),

renamed as(
select
    userId::varchar as user_id,
    adjustId::varchar as adjust_id,
    installedAt::timestamp as install_timestamp,
    trim(lower(countryCode)) as country_code,
    trim(lower(os_name)) as os_name,
    trim(lower(channel)) as acquisition_channel,
    trim(lower(campaign)) as campaign_name,
    trim(lower(creative)) as campaign_creative,
    trim(lower(adGroupName)) as ad_group_name,
    trim(lower(trackerName)) as tracker_name,
    device,
    osVersion as os_version,
    limitAdTracking::boolean as limit_ad_tracking,
    idfa,
    idfv,
    googleAdId as google_ad_id
from source
)

select * 
from renamed
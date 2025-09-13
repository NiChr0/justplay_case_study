with source as(
    select *
    from {{ ref('revenue_raw') }}
),

renamed as(
select
    eventId::varchar as event_id,
    userId::varchar as justplay_user_id,
    deviceId as device_id,
    advertisingId as advertising_id,
    createdAt::timestamp as created_at,
    trim(lower(countryCode)) as country_code,
    trim(lower(platform)) as os_name,
    amount as revenue_usd,
    trim(lower(source)) as revenue_source,
    trim(lower(network)) as ad_network_name,
    trim(lower(adUnitFormat)) as ad_unit_format,
    trim(lower(adUnitName)) as ad_unit_name,
    packageName as package_name
from source
)

select * 
from renamed
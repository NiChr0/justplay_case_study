with source as(
    select *
    from {{ ref('revenue_raw') }}
),

renamed as(
select
    eventId::varchar as event_id,
    userId::varchar as user_id,
    createdAt::timestamp as revenue_timestamp,
    trim(lower(countryCode)) as country_code,
    trim(lower(platform)) as platform,
    coalesce(amount, 0) as revenue_usd,
    trim(lower(source)) as revenue_source,
    trim(lower(network)) as ad_network,
    trim(lower(adUnitFormat)) as ad_unit_format,
    trim(lower(adUnitName)) as ad_unit_name,
    packageName as package_name,
    deviceId as device_id,
    advertisingId as advertising_id
from source
)

select * 
from renamed
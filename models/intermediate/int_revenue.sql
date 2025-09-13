{{
  config(
    materialized = 'table'
    )
}}

with source as(
    select *
    from {{ ref('stg_raw__revenue') }}
),

transformed as(

    select
        event_id,
        justplay_user_id,
        device_id,
        advertising_id,
        created_at,
        country_code,
        os_name,
        revenue_usd,
        revenue_source,
        ad_network_name,
        ad_unit_format,
        ad_unit_name,
        package_name
    from source
    {{dbt_utils.group_by(n=13)}}
)

select *
from transformed
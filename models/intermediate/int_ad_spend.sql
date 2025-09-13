{{
  config(
    materialized = 'table'
    )
}}

with source as(
    select *
    from {{ ref('stg_raw__ad_spend') }}
),

transformed as(

    select
        report_date,
        country_code,
        os_name,
        acquisition_channel,
        campaign_name,
        campaign_creative,
        ad_spend_usd,
        network_clicks,
        network_impressions,
        network_installs,
        adjust_installs
    from source
    {{dbt_utils.group_by(n=11)}}
)

select 
    {{dbt_utils.generate_surrogate_key(['report_date', 'country_code', 'os_name', 'acquisition_channel', 'campaign_name', 'campaign_creative'])}} as ad_spend_id,
    *
from transformed
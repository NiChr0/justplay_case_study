{{
  config(
    materialized = 'incremental',
    unique_key = 'install_id',
    on_schema_change = 'append'
  )
}}

with source as(
    select *
    from {{ ref('stg_raw__installs') }}
    {% if is_incremental() %}
      where installed_at::date > current_date - 7
    {% endif %}
),

transformed as(
    select
        justplay_user_id,
        adjust_user_id,
        installed_at,
        country_code,
        os_name,
        acquisition_channel,
        campaign_name,
        campaign_creative,
        ad_group_name,
        tracker_name,
        device_name,
        os_version,
        has_limit_ad_tracking,
        idfa,
        idfv,
        google_ad_id
    from source
    {{dbt_utils.group_by(n=16)}}
)

select 
    {{dbt_utils.generate_surrogate_key(['justplay_user_id', 'adjust_user_id', 'installed_at'])}} as install_id,
    *
from transformed
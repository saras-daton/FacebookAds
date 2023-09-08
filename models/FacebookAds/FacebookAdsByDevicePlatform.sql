{% if var('FacebookAdsByDevicePlatform') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}


{% set table_name_query %}
{{set_table_name('%facebookads%adinsights%device_platform')}}    
{% endset %}  


{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * {{exclude()}} (row_num)
    FROM (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        account_currency,
        account_id,
        account_name,
        --actions,
        --action_values,
        coalesce(ad_id,'') as ad_id,
        ad_name,
        adset_id,
        adset_name,
        buying_type,
        campaign_id,
        campaign_name,
        device_platform,
        canvas_avg_view_percent,
        CAST(clicks as INT64) clicks,
        cost_per_estimated_ad_recallers,
        cost_per_inline_link_click,
        cost_per_inline_post_engagement,
        cost_per_unique_click,
        cost_per_unique_inline_link_click,
        --conversion_values,
        --conversions,
        cpc,
        cpm,
        cpp,
        ctr,
        CAST(date_start as DATE) date_start,
        CAST(date_stop as DATE) date_stop,
        estimated_ad_recall_rate,
        estimated_ad_recallers,
        frequency,
        CAST(impressions as INT64) impressions,
        CAST(inline_link_clicks as INT64) inline_link_clicks,
        inline_post_engagement,
        objective,
        reach,
        social_spend,
        spend,
        CAST(unique_clicks as INT64) unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        CAST(unique_inline_link_clicks as INT64) unique_inline_link_clicks,

         {% if target.type =='snowflake' %}
        video_30_sec_watched_actions.VALUE:action_type as video_30_sec_watched_actions_action_type,
        video_30_sec_watched_actions.VALUE:value as video_30_sec_watched_actions_value,
        {% else %}
        video_30_sec_watched_actions.action_type as video_30_sec_watched_actions_action_type,
        video_30_sec_watched_actions.values as video_30_sec_watched_actions_value,
        {% endif %}

        {% if target.type =='snowflake' %}
        video_p25_watched_actions.VALUE:action_type as video_p25_watched_actions_action_type,
        video_p25_watched_actions.VALUE:value as video_p25_watched_actions_value,
        {% else %}
        video_p25_watched_actions.action_type as video_p25_watched_actions_action_type,
        video_p25_watched_actions.values as video_p25_watched_actions_value,
        {% endif %}

        {% if target.type =='snowflake' %}
        video_p50_watched_actions.VALUE:action_type as video_p50_watched_actions_action_type,
        video_p50_watched_actions.VALUE:value as video_p50_watched_actions_value,
        {% else %}
        video_p50_watched_actions.action_type as video_p50_watched_actions_action_type,
        video_p50_watched_actions.values as video_p50_watched_actions_value,
        {% endif %}

        {% if target.type =='snowflake' %}
        video_p75_watched_actions.VALUE:action_type as video_p75_watched_actions_action_type,
        video_p75_watched_actions.VALUE:value as video_p75_watched_actions_value,
        {% else %}
        video_p75_watched_actions.action_type as video_p75_watched_actions_action_type,
        video_p75_watched_actions.values as video_p75_watched_actions_value,
        {% endif %}

        {% if target.type =='snowflake' %}
        video_p95_watched_actions.VALUE:action_type as video_p95_watched_actions_action_type,
        video_p95_watched_actions.VALUE:value as video_p95_watched_actions_value,
        {% else %}
        video_p95_watched_actions.action_type as video_p95_watched_actions_action_type,
        video_p95_watched_actions.values as video_p95_watched_actions_value,
        {% endif %}
        
        {% if target.type =='snowflake' %}
        video_p100_watched_actions.VALUE:action_type as video_p100_watched_actions_action_type,
        video_p100_watched_actions.VALUE:value as video_p100_watched_actions_value,
        {% else %}
        video_p100_watched_actions.action_type as video_p100_watched_actions_action_type,
        video_p100_watched_actions.values as video_p100_watched_actions_value,
        {% endif %}

        {% if target.type =='snowflake' %}
        video_avg_time_watched_actions.VALUE:action_type as video_avg_time_watched_actions_action_type,
        video_avg_time_watched_actions.VALUE:value as video_avg_time_watched_actions_value,
        {% else %}
        video_avg_time_watched_actions.action_type as video_avg_time_watched_actions_action_type,
        video_avg_time_watched_actions.values as video_avg_time_watched_actions_value,
        {% endif %}

        
        {% if target.type =='snowflake' %}
        video_play_actions.VALUE:action_type as video_play_actions_action_type,
        video_play_actions.VALUE:value as video_play_actions_value,
        video_play_actions.VALUE:_d_view as video_play_actions_d_view,
        {% else %}
        video_play_actions.action_type as video_play_actions_action_type,
        video_play_actions.values as video_play_actions_value,
        video_play_actions._d_view as video_play_actions_d_view,
        {% endif %}

        {% if target.type =='snowflake' %}
        website_ctr.VALUE:"1d_click"::VARCHAR as website_ctr_1d_click,
        website_ctr.VALUE:"1d_view"::VARCHAR as website_ctr_1d_view,
        website_ctr.VALUE:"7d_click"::VARCHAR as website_ctr_7d_click,
        website_ctr.VALUE:"7d_view"::VARCHAR as website_ctr_7d_view,
        website_ctr.VALUE:"28d_click"::VARCHAR as website_ctr_28d_click,
        website_ctr.VALUE:"28d_view"::VARCHAR as website_ctr_28d_view,
        website_ctr.VALUE:action_canvas_component_id::VARCHAR as website_ctr_action_canvas_component_id,
        website_ctr.VALUE:action_canvas_component_name::VARCHAR as website_ctr_action_canvas_component_name,
        website_ctr.VALUE:action_carousel_card_id::VARCHAR as website_ctr_action_carousel_card_id,
        website_ctr.VALUE:action_carousel_card_name::VARCHAR as website_ctr_action_carousel_card_name,
        website_ctr.VALUE:action_destination::VARCHAR as website_ctr_action_destination,
        website_ctr.VALUE:action_device::VARCHAR as website_ctr_action_device,
        website_ctr.VALUE:action_event_channel::VARCHAR as website_ctr_action_event_channel,
        website_ctr.VALUE:action_link_click_destination::VARCHAR as website_ctr_action_link_click_destination,
        website_ctr.VALUE:action_location_code::VARCHAR as website_ctr_action_location_code,
        website_ctr.VALUE:action_reaction::VARCHAR as website_ctr_action_reaction,
        website_ctr.VALUE:action_target_id::VARCHAR as website_ctr_action_target_id,
        coalesce(website_ctr.VALUE:action_type::VARCHAR,'') as website_ctr_action_type,
        website_ctr.VALUE:action_video_asset_id::VARCHAR as website_ctr_action_video_asset_id,
        website_ctr.VALUE:action_video_sound::VARCHAR as website_ctr_action_video_sound,
        website_ctr.VALUE:action_video_type::VARCHAR as website_ctr_action_video_type,
        website_ctr.VALUE:inline::VARCHAR as website_ctr_inline,
        website_ctr.VALUE:value::VARCHAR as website_ctr_value,
        website_ctr.VALUE:id::VARCHAR as website_ctr_id,
        {% else %}
        website_ctr._daton_pre_1d_click as website_ctr_1d_click,
        website_ctr._daton_pre_1d_view as website_ctr_1d_view,
        website_ctr._daton_pre_7d_click as website_ctr_7d_click,
        website_ctr._daton_pre_7d_view as website_ctr_7d_view,
        website_ctr._daton_pre_28d_click as website_ctr_28d_click,
        website_ctr._daton_pre_28d_view as website_ctr_28d_view,
        website_ctr.action_canvas_component_id as website_ctr_action_canvas_component_id,
        website_ctr.action_canvas_component_name as website_ctr_action_canvas_component_name,
        website_ctr.action_carousel_card_id as website_ctr_action_carousel_card_id,
        website_ctr.action_carousel_card_name as website_ctr_action_carousel_card_name,
        website_ctr.action_destination as website_ctr_action_destination,
        website_ctr.action_device as website_ctr_action_device,
        website_ctr.action_event_channel as website_ctr_action_event_channel,
        website_ctr.action_link_click_destination as website_ctr_action_link_click_destination,
        website_ctr.action_location_code as website_ctr_action_location_code,
        website_ctr.action_reaction as website_ctr_action_reaction,
        website_ctr.action_target_id as website_ctr_action_target_id,
        website_ctr.action_type as website_ctr_action_type,
        website_ctr.action_video_asset_id as website_ctr_action_video_asset_id,
        website_ctr.action_video_sound as website_ctr_action_video_sound,
        website_ctr.action_video_type as website_ctr_action_video_type,
        website_ctr.inline as website_ctr_inline,
        website_ctr.value as website_ctr_value,
        website_ctr.id as website_ctr_id,
        {% endif %}

        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then account_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            account_currency as exchange_currency_code, 
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY ad_id,date_start,device_platform order by a.{{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a  
                {{unnesting("video_30_sec_watched_actions")}}
                {{unnesting("video_p25_watched_actions")}}
                {{unnesting("video_p50_watched_actions")}}
                {{unnesting("video_p75_watched_actions")}}
                {{unnesting("video_p95_watched_actions")}}
                {{unnesting("video_p100_watched_actions")}}
                {{unnesting("video_avg_time_watched_actions")}}
                {{unnesting("video_play_actions")}}
                {{unnesting("website_ctr")}}

                {% if var('currency_conversion_flag') %}
                    left join {{ref('ExchangeRates')}} c on date(date_start) = c.date and account_currency = c.to_currency_code
                {% endif %}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}

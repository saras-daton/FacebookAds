{% if var('FacebookAdinsightsConversions') %}
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
{{set_table_name('%facebookads%adinsights')}}    
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

with fbadinsights as (
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

    SELECT * 
    From (
        select
        '{{brand}}' as brand,
        '{{store}}' as store,
        {% if var('currency_conversion_flag') %}
            case when c.value is null then 1 else c.value end as exchange_currency_rate,
            case when c.from_currency_code is null then account_currency else c.from_currency_code end as exchange_currency_code,
        {% else %}
            cast(1 as decimal) as exchange_currency_rate,
            cast(null as string) as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        account_currency,
        account_id,
        account_name,
        actions,
        action_values,
        coalesce(ad_id,'') as ad_id,
        ad_name,
        adset_id,
        adset_name,
        buying_type,
        campaign_id,
        campaign_name,
        canvas_avg_view_percent,
        clicks,
        cost_per_estimated_ad_recallers,
        cost_per_inline_link_click,
        cost_per_inline_post_engagement,
        cost_per_unique_click,
        cost_per_unique_inline_link_click,
        conversion_values,
        {% if target.type =='snowflake' %}
        CONVERSIONS.VALUE:"1d_click" as conversions_1d_click,
        CONVERSIONS.VALUE:"1d_view" as conversions_1d_view,
        CONVERSIONS.VALUE:"7d_click" as conversions_7d_click,
        CONVERSIONS.VALUE:"7d_view" as conversions_7d_views,
        CONVERSIONS.VALUE:"28d_click" as conversions_28d_click,
        CONVERSIONS.VALUE:"28d_view" as conversions_28d_view,
        CONVERSIONS.VALUE:action_canvas_component_id as conversions_action_canvas_component_id,
        CONVERSIONS.VALUE:action_canvas_component_name as conversions_action_canvas_component_name,
        CONVERSIONS.VALUE:action_carousel_card_id as conversions_action_carousel_card_id,
        CONVERSIONS.VALUE:action_carousel_card_name as conversions_action_carousel_card_name,
        CONVERSIONS.VALUE:action_destination as conversions_action_destination,
        CONVERSIONS.VALUE:action_device as conversions_action_device,
        CONVERSIONS.VALUE:action_event_channel as conversions_action_event_channel,
        CONVERSIONS.VALUE:action_link_click_destination as conversions_action_link_click_destination,
        CONVERSIONS.VALUE:action_location_code as conversions_action_location_code,
        CONVERSIONS.VALUE:action_reaction as conversions_action_reaction,
        CONVERSIONS.VALUE:action_target_id as conversions_action_target_id,
        CONVERSIONS.VALUE:action_type as conversions_action_type,
        CONVERSIONS.VALUE:action_video_asset_id as conversions_action_video_asset_id,
        CONVERSIONS.VALUE:action_video_sound as conversions_action_video_sound,
        CONVERSIONS.VALUE:action_video_type as conversions_action_video_type,
        CONVERSIONS.VALUE:inline as conversions_inline,
        CONVERSIONS.VALUE:value as conversions_value,
        CONVERSIONS.VALUE:id as conversions_id,
        {% else %}
        conversions._daton_pre_1d_click as conversions_1d_click,
        conversions._daton_pre_1d_view as conversions_1d_view,
        conversions._daton_pre_7d_click as conversions_7d_click,
        conversions._daton_pre_7d_view as conversions_7d_views,
        conversions._daton_pre_28d_click as conversions_28d_click,
        conversions._daton_pre_28d_view as conversions_28d_view,
        conversions.action_canvas_component_id as conversions_action_canvas_component_id,
        conversions.action_canvas_component_name as conversions_action_canvas_component_name,
        conversions.action_carousel_card_id as conversions_action_carousel_card_id,
        conversions.action_carousel_card_name as conversions_action_carousel_card_name,
        conversions.action_destination as conversions_action_destination,
        conversions.action_device as conversions_action_device,
        conversions.action_event_channel as conversions_action_event_channel,
        conversions.action_link_click_destination as conversions_action_link_click_destination,
        conversions.action_location_code as conversions_action_location_code,
        conversions.action_reaction as conversions_action_reaction,
        conversions.action_target_id as conversions_action_target_id,
        conversions.action_type as conversions_action_type,
        conversions.action_video_asset_id as conversions_action_video_asset_id,
        conversions.action_video_sound as conversions_action_video_sound,
        conversions.action_video_type as conversions_action_video_type,
        conversions.inline as conversions_inline,
        conversions.value as conversions_value,
        conversions.id as conversions_id,
        {% endif %}
        cpc,
        cpm,
        cpp,
        ctr,
        CAST(date_start as DATE) date_start,
        CAST(date_stop as DATE) date_stop,
        estimated_ad_recall_rate,
        estimated_ad_recallers,
        frequency,
        impressions,
        inline_link_clicks,
        inline_post_engagement,
        objective,
        reach,
        social_spend,
        spend,
        unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        unique_inline_link_clicks,
        website_ctr,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,CONVERSIONS.VALUE:action_type order by {{daton_batch_runtime()}} desc) row_num
        {% else %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,conversions.action_type order by {{daton_batch_runtime()}} desc) row_num
        {% endif %}
        from {{i}}
            {{unnesting("CONVERSIONS")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}
    
        ) a
        {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(date_start) = c.date and account_currency = c.to_currency_code
        {% endif %}
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select * {{exclude()}}(row_num)
from fbadinsights 
where row_num =1


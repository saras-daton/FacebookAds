{% if var('FacebookAdInsightsActionValues') %}
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
            account_currency as exchange_currency_code, 
        {% endif %}
        a.* from (
        select 
        account_currency,
        account_id,
        account_name,
        --actions,
        {% if target.type =='snowflake' %}
        ACTION_VALUES.VALUE:"1d_click"::VARCHAR as action_values_1d_click,
        ACTION_VALUES.VALUE:"1d_view"::VARCHAR as action_values_1d_view,
        ACTION_VALUES.VALUE:"7d_click"::VARCHAR as action_values_7d_click,
        ACTION_VALUES.VALUE:"7d_view"::VARCHAR as action_values_7d_view,
        ACTION_VALUES.VALUE:"28d_click"::VARCHAR as action_values_28d_click,
        ACTION_VALUES.VALUE:"28d_view"::VARCHAR as action_values_28d_view,
        ACTION_VALUES.VALUE:action_canvas_component_id::VARCHAR as action_values_action_canvas_component_id,
        ACTION_VALUES.VALUE:action_canvas_component_name::VARCHAR as action_values_action_canvas_component_name,
        ACTION_VALUES.VALUE:action_carousel_card_id::VARCHAR as action_values_action_carousel_card_id,
        ACTION_VALUES.VALUE:action_carousel_card_name::VARCHAR as action_values_action_carousel_card_name,
        ACTION_VALUES.VALUE:action_destination::VARCHAR as action_values_action_destination,
        ACTION_VALUES.VALUE:action_device::VARCHAR as action_values_action_device,
        ACTION_VALUES.VALUE:action_event_channel::VARCHAR as action_values_action_event_channel,
        ACTION_VALUES.VALUE:action_link_click_destination::VARCHAR as action_values_action_link_click_destination,
        ACTION_VALUES.VALUE:action_location_code::VARCHAR as action_values_action_location_code,
        ACTION_VALUES.VALUE:action_reaction::VARCHAR as action_values_action_reaction,
        ACTION_VALUES.VALUE:action_target_id::VARCHAR as action_values_action_target_id,
        coalesce(ACTION_VALUES.VALUE:action_type::VARCHAR,'') as action_values_action_type,
        ACTION_VALUES.VALUE:action_video_asset_id::VARCHAR as action_values_action_video_asset_id,
        ACTION_VALUES.VALUE:action_video_sound::VARCHAR as action_values_action_video_sound,
        ACTION_VALUES.VALUE:action_video_type::VARCHAR as action_values_action_video_type,
        ACTION_VALUES.VALUE:inline::VARCHAR as action_values_inline,
        ACTION_VALUES.VALUE:value::VARCHAR as action_values_value,
        ACTION_VALUES.VALUE:id::VARCHAR as action_values_id,
        {% else %}
        action_values._daton_pre_1d_click as action_values_1d_click,
        action_values._daton_pre_1d_view as action_values_1d_view,
        action_values._daton_pre_7d_click as action_values_7d_click,
        action_values._daton_pre_7d_view as action_values_7d_view,
        action_values._daton_pre_28d_click as action_values_28d_click,
        action_values._daton_pre_28d_view as action_values_28d_view,
        action_values.action_canvas_component_id as action_values_action_canvas_component_id,
        action_values.action_canvas_component_name as action_values_action_canvas_component_name,
        action_values.action_carousel_card_id as action_values_action_carousel_card_id,
        action_values.action_carousel_card_name as action_values_action_carousel_card_name,
        action_values.action_destination as action_values_action_destination,
        action_values.action_device as action_values_action_device,
        action_values.action_event_channel as action_values_action_event_channel,
        action_values.action_link_click_destination as action_values_action_link_click_destination,
        action_values.action_location_code as action_values_action_location_code,
        action_values.action_reaction as action_values_action_reaction,
        action_values.action_target_id as action_values_action_target_id,
        action_values.action_type as action_values_action_type,
        action_values.action_video_asset_id as action_values_action_video_asset_id,
        action_values.action_video_sound as action_values_action_video_sound,
        action_values.action_video_type as action_values_action_video_type,
        action_values.inline as action_values_inline,
        action_values.value as action_values_value,
        action_values.id as action_values_id,
        {% endif %}
        coalesce(ad_id,'') as ad_id,
        ad_name,
        adset_id,
        adset_name,
        buying_type,
        campaign_id,
        campaign_name,
        --canvas_avg_view_percent,
        --clicks,
        --cost_per_estimated_ad_recallers,
        --cost_per_inline_link_click,
        --cost_per_inline_post_engagement,
        --cost_per_unique_click,
        --cost_per_unique_inline_link_click,
        --conversion_values,
        --conversions,
        --cpc,
        --cpm,
        --cpp,
        --ctr,
        CAST(date_start as DATE) date_start,
        CAST(date_stop as DATE) date_stop,
        --estimated_ad_recall_rate,
        --estimated_ad_recallers,
        --frequency,
        --impressions,
        --inline_link_clicks,
        --inline_post_engagement,
        objective,
        --reach,
        --social_spend,
        --spend,
        --unique_clicks,
        --unique_ctr,
        --unique_inline_link_click_ctr,
        --unique_inline_link_clicks,
        --website_ctr,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        {% if target.type =='snowflake' %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,action_values.VALUE:action_type order by {{daton_batch_runtime()}} desc) row_num
        {% else %}
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,action_values.action_type order by {{daton_batch_runtime()}} desc) row_num
        {% endif %}
        from {{i}}
            {{unnesting("ACTION_VALUES")}}
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


-- depends_on: {{ ref('ExchangeRates') }}

{% if var('table_partition_flag') %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    partition_by = { 'field': 'date_start', 'data_type': 'date' },
    cluster_by = ['date_start'],
    unique_key = ['date_start', 'ad_id', 'action_type'])}}
{% else %}
{{config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['date_start', 'ad_id','action_type'])}}
{% endif %}

{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX({{daton_batch_runtime()}} ) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}

with fbadinsights as (
{% set table_name_query %}
{{set_table_name('%facebookads%adinsights')}}    
{% endset %}  



{% set results = run_query(table_name_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{% if var('timezone_conversion_flag') %}
    {% set hr = var('timezone_conversion_hours') %}
{% endif %}

{% for i in results_list %}
    {% if var('brand_consolidation_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brand_name_position')] %}
    {% else %}
        {% set brand = var('brand_name') %}
    {% endif %}

    {% if var('store_consolidation_flag') %}
        {% set store =i.split('.')[2].split('_')[var('store_name_position')] %}
    {% else %}
        {% set store = var('store') %}
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
        ACTIONS,
        {% if var('snowflake_database_flag') %} 
        --action_values._daton_pre_1d_click,
        --action_values._daton_pre_1d_view,
        --action_values._daton_pre_28d_click,
        --action_values._daton_pre_28d_view,
        --action_values._daton_pre_7d_click,
        --action_values._daton_pre_7d_view,
        ACTION_VALUES.VALUE:action_canvas_component_id::VARCHAR as action_canvas_component_id,
        ACTION_VALUES.VALUE:action_canvas_component_name::VARCHAR as action_canvas_component_name,
        ACTION_VALUES.VALUE:action_carousel_card_id::VARCHAR as action_carousel_card_id,
        ACTION_VALUES.VALUE:action_carousel_card_name::VARCHAR as action_carousel_card_name,
        ACTION_VALUES.VALUE:action_destination::VARCHAR as action_destination,
        ACTION_VALUES.VALUE:action_device::VARCHAR as action_device,
        ACTION_VALUES.VALUE:action_event_channel::VARCHAR as action_event_channel,
        ACTION_VALUES.VALUE:action_link_click_destination::VARCHAR as action_link_click_destination,
        ACTION_VALUES.VALUE:action_location_code::VARCHAR as action_location_code,
        ACTION_VALUES.VALUE:action_reaction::VARCHAR as action_reaction,
        ACTION_VALUES.VALUE:action_target_id::VARCHAR as action_target_id,
        coalesce(ACTION_VALUES.VALUE:action_type::VARCHAR,'') as action_type,
        ACTION_VALUES.VALUE:action_video_asset_id::VARCHAR as action_video_asset_id,
        ACTION_VALUES.VALUE:action_video_sound::VARCHAR as action_video_sound,
        ACTION_VALUES.VALUE:action_video_type::VARCHAR as action_video_type,
        ACTION_VALUES.VALUE:inline::VARCHAR as inline,
        ACTION_VALUES.VALUE:value::VARCHAR as value,
        ACTION_VALUES.VALUE:id::VARCHAR as id,
        {% else %}
        action_values._daton_pre_1d_click,
        action_values._daton_pre_1d_view,
        action_values._daton_pre_28d_click,
        action_values._daton_pre_28d_view,
        action_values._daton_pre_7d_click,
        action_values._daton_pre_7d_view,
        action_values.action_canvas_component_id,
        action_values.action_canvas_component_name,
        action_values.action_carousel_card_id,
        action_values.action_carousel_card_name,
        action_values.action_destination,
        action_values.action_device,
        action_values.action_event_channel,
        action_values.action_link_click_destination,
        action_values.action_location_code,
        action_values.action_reaction,
        action_values.action_target_id,
        action_values.action_type,
        action_values.action_video_asset_id,
        action_values.action_video_sound,
        action_values.action_video_type,
        action_values.inline,
        action_values.value,
        action_values.id,
        {% endif %}
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
        cpc,
        cpm,
        cpp,
        ctr,
        CAST(date_start as DATE) date_start,
        CAST(date_stop as DATE) date_stop,
        estimated_ad_recallers,
        frequency,
        inline_post_engagement,
        reach,
        social_spend,
        spend,
        unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        unique_inline_link_clicks,
        {{daton_user_id()}},
        {{daton_batch_runtime()}},
        {{daton_batch_id()}},
        {% if var('timezone_conversion_flag') %}
            DATETIME_ADD(cast(date_start as timestamp), INTERVAL {{hr}} HOUR ) as effective_start_date,
            null as effective_end_date,
            DATETIME_ADD(current_timestamp(), INTERVAL {{hr}} HOUR ) as last_updated,
            null as run_id,
        {% else %}
            cast(date_start as timestamp) as effective_start_date,
            null as effective_end_date,
            current_timestamp() as last_updated,
            null as run_id,
        {% endif %}
        {% if var('snowflake_database_flag') %} 
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


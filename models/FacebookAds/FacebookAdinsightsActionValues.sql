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

        {{extract_nested_value("action_values","_daton_pre_1d_click","string")}} as action_values_1d_click,
        {{extract_nested_value("action_values","_daton_pre_1d_view","string")}} as action_values_1d_view,
        {{extract_nested_value("action_values","_daton_pre_28d_click","string")}} as action_values_28d_click,
        {{extract_nested_value("action_values","_daton_pre_28d_view","string")}} as action_values_28d_view,
        {{extract_nested_value("action_values","_daton_pre_7d_click","string")}} as action_values_7d_click,
        {{extract_nested_value("action_values","_daton_pre_7d_view","string")}} as action_values_7d_view,
        {{extract_nested_value("action_values","action_canvas_component_id","string")}} as action_values_action_canvas_component_id,
        {{extract_nested_value("action_values","action_canvas_component_name","string")}} as action_values_action_canvas_component_name,
        {{extract_nested_value("action_values","action_carousel_card_id","string")}} as action_values_action_carousel_card_id,
        {{extract_nested_value("action_values","action_carousel_card_name","string")}} as action_values_action_carousel_card_name,
        {{extract_nested_value("action_values","action_destination","string")}} as action_values_action_destination,
        {{extract_nested_value("action_values","action_device","string")}} as action_values_action_device,
        {{extract_nested_value("action_values","action_event_channel","string")}} as action_values_action_event_channel,
        {{extract_nested_value("action_values","action_link_click_destination","string")}} as action_values_action_link_click_destination,
        {{extract_nested_value("action_values","action_location_code","string")}} as action_values_action_location_code,
        {{extract_nested_value("action_values","action_reaction","string")}} as action_values_action_reaction,
        {{extract_nested_value("action_values","action_target_id","string")}} as action_values_action_target_id,
        coalesce{{extract_nested_value("action_values","action_type","string")}},'NA') as action_values_action_type,
        {{extract_nested_value("action_values","action_video_asset_id","string")}} as action_values_action_video_asset_id,
        {{extract_nested_value("action_values","action_video_sound","string")}} as action_values_action_video_sound,
        {{extract_nested_value("action_values","action_video_type","string")}} as action_values_action_video_type,
        {{extract_nested_value("action_values","inline","string")}} as action_values_inline,
        {{extract_nested_value("action_values","value","string")}} as action_values_value,
        {{extract_nested_value("action_values","id","string")}} as action_values_id,

        coalesce(ad_id,'NA') as ad_id,
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
        cast(coalesce(date_start,'NA')as Date) date_start,
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
        ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,{{extract_nested_value("action_values","action_type","string")}} order by {{daton_batch_runtime()}} desc) row_num
        from {{i}}
            {{unnesting("action_values")}}
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


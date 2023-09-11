{% if var('FacebookAdsByRegion') %}
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
{{set_table_name('%facebookads%adinsights%breakdown_region')}}    
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
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,region order by _daton_batch_runtime desc) _seq_id FROM (
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
        coalesce(ad_id,'NA') as ad_id,
        ad_name,
        adset_id,
        adset_name,
        buying_type,
        campaign_id,
        campaign_name,
        coalesce(region,'NA') as region,
        canvas_avg_view_percent,
        CAST(clicks as INT64) as clicks,
        cost_per_estimated_ad_recallers,
        cost_per_inline_link_click,
        cost_per_inline_post_engagement,
        cost_per_unique_click,
        cost_per_unique_inline_link_click,
        --conversion_values,

        {{extract_nested_value("conversions","_daton_pre_1d_click","string")}} as conversions_1d_click,
        {{extract_nested_value("conversions","_daton_pre_1d_view","string")}} as conversions_1d_view,
        {{extract_nested_value("conversions","_daton_pre_28d_click","string")}} as conversions_28d_click,
        {{extract_nested_value("conversions","_daton_pre_28d_view","string")}} as conversions_28d_view,
        {{extract_nested_value("conversions","_daton_pre_7d_click","string")}} as conversions_7d_click,
        {{extract_nested_value("conversions","_daton_pre_7d_view","string")}} as conversions_7d_view,
        {{extract_nested_value("conversions","action_canvas_component_id","string")}} as conversions_action_canvas_component_id,
        {{extract_nested_value("conversions","action_canvas_component_name","string")}} as conversions_action_canvas_component_name,
        {{extract_nested_value("conversions","action_carousel_card_id","string")}} as conversions_action_carousel_card_id,
        {{extract_nested_value("conversions","action_carousel_card_name","string")}} as conversions_action_carousel_card_name,
        {{extract_nested_value("conversions","action_destination","string")}} as conversions_action_destination,
        {{extract_nested_value("conversions","action_device","string")}} as conversions_action_device,
        {{extract_nested_value("conversions","action_event_channel","string")}} as conversions_action_event_channel,
        {{extract_nested_value("conversions","action_link_click_destination","string")}} as conversions_action_link_click_destination,
        {{extract_nested_value("conversions","action_location_code","string")}} as conversions_action_location_code,
        {{extract_nested_value("conversions","action_reaction","string")}} as conversions_action_reaction,
        {{extract_nested_value("conversions","action_target_id","string")}} as conversions_action_target_id,
        {{extract_nested_value("conversions","action_type","string")}} as conversions_action_type,
        {{extract_nested_value("conversions","action_video_asset_id","string")}} as conversions_action_video_asset_id,
        {{extract_nested_value("conversions","action_video_sound","string")}} as conversions_action_video_sound,
        {{extract_nested_value("conversions","action_video_type","string")}} as conversions_action_video_type,
        {{extract_nested_value("conversions","inline","string")}} as conversions_inline,
        {{extract_nested_value("conversions","value","string")}} as conversions_value,
        {{extract_nested_value("conversions","id","string")}} as conversions_id,

        cpc,
        cpm,
        cpp,
        ctr,
        cast(coalesce(date_start,'NA')as Date) as date_start,
        CAST(date_stop as DATE) date_stop,
        estimated_ad_recall_rate,
        estimated_ad_recallers,
        frequency,
        CAST(impressions as INT64) as impressions,
        CAST(inline_link_clicks as INT64) as inline_link_clicks,
        inline_post_engagement,
        objective,
        reach,
        social_spend,
        spend,
        CAST(unique_clicks as INT64) as unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        CAST(unique_inline_link_clicks as INT64) as unique_inline_link_clicks,
    
        {{extract_nested_value("website_ctr","_daton_pre_1d_click","string")}} as website_ctr_1d_click,
        {{extract_nested_value("website_ctr","_daton_pre_1d_view","string")}} as website_ctr_1d_view,
        {{extract_nested_value("website_ctr","_daton_pre_28d_click","string")}} as website_ctr_28d_click,
        {{extract_nested_value("website_ctr","_daton_pre_28d_view","string")}} as website_ctr_28d_view,
        {{extract_nested_value("website_ctr","_daton_pre_7d_click","string")}} as website_ctr_7d_click,
        {{extract_nested_value("website_ctr","_daton_pre_7d_view","string")}} as website_ctr_7d_view,
        {{extract_nested_value("website_ctr","action_canvas_component_id","string")}} as website_ctr_action_canvas_component_id,
        {{extract_nested_value("website_ctr","action_canvas_component_name","string")}} as website_ctr_action_canvas_component_name,
        {{extract_nested_value("website_ctr","action_carousel_card_id","string")}} as website_ctr_action_carousel_card_id,
        {{extract_nested_value("website_ctr","action_carousel_card_name","string")}} as website_ctr_action_carousel_card_name,
        {{extract_nested_value("website_ctr","action_destination","string")}} as website_ctr_action_destination,
        {{extract_nested_value("website_ctr","action_device","string")}} as website_ctr_action_device,
        {{extract_nested_value("website_ctr","action_event_channel","string")}} as website_ctr_action_event_channel,
        {{extract_nested_value("website_ctr","action_link_click_destination","string")}} as website_ctr_action_link_click_destination,
        {{extract_nested_value("website_ctr","action_location_code","string")}} as website_ctr_action_location_code,
        {{extract_nested_value("website_ctr","action_reaction","string")}} as website_ctr_action_reaction,
        {{extract_nested_value("website_ctr","action_target_id","string")}} as website_ctr_action_target_id,
        {{extract_nested_value("website_ctr","action_type","string")}} as website_ctr_action_type,
        {{extract_nested_value("website_ctr","action_video_asset_id","string")}} as website_ctr_action_video_asset_id,
        {{extract_nested_value("website_ctr","action_video_sound","string")}} as website_ctr_action_video_sound,
        {{extract_nested_value("website_ctr","action_video_type","string")}} as website_ctr_action_video_type,
        {{extract_nested_value("website_ctr","inline","string")}} as website_ctr_inline,
        {{extract_nested_value("website_ctr","value","string")}} as website_ctr_value,
        {{extract_nested_value("website_ctr","id","string")}} as website_ctr_id,

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
        DENSE_RANK() OVER (PARTITION BY ad_id,date_start,region order by a.{{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
                {{unnesting("conversions")}}
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
    )
    {% if not loop.last %} union all {% endif %}
{% endfor %}

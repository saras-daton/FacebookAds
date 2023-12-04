{% if var('FacebookAdinsightsConversionValues') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("fbads_insights_conversionvalues_tbl_ptrn","fbads_insights_conversionvalues_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        {#/*Currency_conversion as exchange_rates alias can be differnt we have value and from_currency_code*/#}
        {{ currency_conversion('c.value', 'c.from_currency_code', 'account_currency') }},
        account_currency,
        account_id,
        account_name,
        --actions,
        --action_values,
        ad_id,
        ad_name,
        adset_id,
        adset_name,
        buying_type,
        campaign_id,
        campaign_name,
        -- canvas_avg_view_percent,
        --clicks,
        --cost_per_estimated_ad_recallers,
        --cost_per_inline_link_click,
        --cost_per_inline_post_engagement,
        --cost_per_unique_click,
        --cost_per_unique_inline_link_click,

        {{extract_nested_value("conversion_values","_daton_pre_1d_click","string")}} as conversion_values_1d_click,
        {{extract_nested_value("conversion_values","_daton_pre_1d_view","string")}} as conversion_values_1d_view,
        {{extract_nested_value("conversion_values","_daton_pre_28d_click","string")}} as conversion_values_28d_click,
        {{extract_nested_value("conversion_values","_daton_pre_28d_view","string")}} as conversion_values_28d_view,
        {{extract_nested_value("conversion_values","_daton_pre_7d_click","string")}} as conversion_values_7d_click,
        {{extract_nested_value("conversion_values","_daton_pre_7d_view","string")}} as conversion_values_7d_view,
        {{extract_nested_value("conversion_values","action_canvas_component_id","string")}} as conversion_values_action_canvas_component_id,
        {{extract_nested_value("conversion_values","action_canvas_component_name","string")}} as conversion_values_action_canvas_component_name,
        {{extract_nested_value("conversion_values","action_carousel_card_id","string")}} as conversion_values_action_carousel_card_id,
        {{extract_nested_value("conversion_values","action_carousel_card_name","string")}} as conversion_values_action_carousel_card_name,
        {{extract_nested_value("conversion_values","action_destination","string")}} as conversion_values_action_destination,
        {{extract_nested_value("conversion_values","action_device","string")}} as conversion_values_action_device,
        {{extract_nested_value("conversion_values","action_event_channel","string")}} as conversion_values_action_event_channel,
        {{extract_nested_value("conversion_values","action_link_click_destination","string")}} as conversion_values_action_link_click_destination,
        {{extract_nested_value("conversion_values","action_location_code","string")}} as conversion_values_action_location_code,
        {{extract_nested_value("conversion_values","action_reaction","string")}} as conversion_values_action_reaction,
        {{extract_nested_value("conversion_values","action_target_id","string")}} as conversion_values_action_target_id,
        {{extract_nested_value("conversion_values","action_type","string")}}as conversion_values_action_type,
        {{extract_nested_value("conversion_values","action_video_asset_id","string")}} as conversion_values_action_video_asset_id,
        {{extract_nested_value("conversion_values","action_video_sound","string")}} as conversion_values_action_video_sound,
        {{extract_nested_value("conversion_values","action_video_type","string")}} as conversion_values_action_video_type,
        {{extract_nested_value("conversion_values","inline","string")}} as conversion_values_inline,
        {{extract_nested_value("conversion_values","value","string")}} as conversion_values_value,
        {{extract_nested_value("conversion_values","id","string")}} as conversion_id,
        --conversions,
        --cpc,
        --cpm,
        --cpp,
        --ctr,
        cast(date_start as Date) date_start,
        cast(date_stop as DATE) date_stop,
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
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        
    from {{i}} a
    {% if var('currency_conversion_flag') %}
            left join {{ref('ExchangeRates')}} c on date(date_start) = c.date and account_currency = c.to_currency_code
    {% endif %}
            {{unnesting("conversion_values")}}
        {% if is_incremental() %}
        {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('fbads_insights_conversionvalues_lookback') }},0) from {{ this }})
        {% endif %}    
    qualify ROW_NUMBER() OVER (PARTITION BY ad_id,date_start,{{extract_nested_value("conversion_values","action_type","string")}} order by {{daton_batch_runtime()}} desc) = 1


{% if not loop.last %} union all {% endif %}
{% endfor %}   



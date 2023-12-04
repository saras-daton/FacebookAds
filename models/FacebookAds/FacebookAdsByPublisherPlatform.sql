{% if var('FacebookAdsByPublisherPlatform') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}

{% if var('currency_conversion_flag') %}
-- depends_on: {{ ref('ExchangeRates') }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("fbads_by_publisher_platform_tbl_ptrn","fbads_by_publisher_platform_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
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
        cast(clicks as BIGINT) as clicks,
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
        cast(date_start as Date) as date_start,
        cast(date_stop as DATE) date_stop,
        frequency,
        cast(impressions as BIGINT) as impressions,
        cast(inline_link_clicks as BIGINT) as inline_link_clicks,
        inline_post_engagement,
        objective,
        reach,
        spend,
        cast(unique_clicks as BIGINT) as unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        cast(unique_inline_link_clicks as BIGINT) as unique_inline_link_clicks,

        {{extract_nested_value("video_30_sec_watched_actions","action_type","string")}} as video_30_sec_watched_actions_action_type,
        {{extract_nested_value("video_30_sec_watched_actions","value","string")}} as video_30_sec_watched_actions_value,
    
        {{extract_nested_value("video_p25_watched_actions","action_type","string")}} as video_p25_watched_actions_action_type,
        {{extract_nested_value("video_p25_watched_actions","value","string")}} as video_p25_watched_actions_value,
       
        {{extract_nested_value("video_p50_watched_actions","action_type","string")}} as video_p50_watched_actions_action_type,
        {{extract_nested_value("video_p50_watched_actions","value","string")}} as video_p50_watched_actions_value,
    
        {{extract_nested_value("video_p75_watched_actions","action_type","string")}} as video_p75_watched_actions_action_type,
        {{extract_nested_value("video_p75_watched_actions","value","string")}} as video_p75_watched_actions_value,
       
        {{extract_nested_value("video_p95_watched_actions","action_type","string")}} as video_p95_watched_actions_action_type,
        {{extract_nested_value("video_p95_watched_actions","value","string")}} as video_p95_watched_actions_value,
    
        {{extract_nested_value("video_p100_watched_actions","action_type","string")}} as video_p100_watched_actions_action_type,
        {{extract_nested_value("video_p100_watched_actions","value","string")}} as video_p100_watched_actions_value,
       
        {{extract_nested_value("video_avg_time_watched_actions","action_type","string")}} as video_avg_time_watched_actions_action_type,
        {{extract_nested_value("video_avg_time_watched_actions","value","string")}} as video_avg_time_watched_actions_value,
       
        {{extract_nested_value("video_play_actions","action_type","string")}} as video_play_actions_action_type,
        {{extract_nested_value("video_play_actions","value","string")}} as video_play_actions_value,
        {{extract_nested_value("video_play_actions","_d_view","string")}} as video_play_actions_d_view,

        {{extract_nested_value("website_ctr","action_type","string")}} as website_ctr_action_type,
        {{extract_nested_value("website_ctr","value","string")}} as website_ctr_value,
        
        publisher_platform,
        {#/*Currency_conversion as exchange_rates alias can be differnt we have value and from_currency_code*/#}
        {{ currency_conversion('c.value', 'c.from_currency_code', 'account_currency') }},

        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
    FROM  {{i}} a
            {% if var('currency_conversion_flag') %}
                left join {{ref('ExchangeRates')}} c on date(date_start) = c.date and account_currency = c.to_currency_code
            {% endif %}
            {{unnesting("video_30_sec_watched_actions")}}
            {{unnesting("video_p25_watched_actions")}}
            {{unnesting("video_p50_watched_actions")}}
            {{unnesting("video_p75_watched_actions")}}
            {{unnesting("video_p95_watched_actions")}}
            {{unnesting("video_p100_watched_actions")}}
            {{unnesting("video_avg_time_watched_actions")}}
            {{unnesting("video_play_actions")}}
            {{unnesting("website_ctr")}}
            {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('fbads_by_publisher_platform_lookback') }},0) from {{ this }})
        {% endif %}    
        qualify DENSE_RANK() OVER (PARTITION BY ad_id,date_start,publisher_platform order by a.{{daton_batch_runtime()}} desc) = 1 
{% if not loop.last %} union all {% endif %}
{% endfor %}  
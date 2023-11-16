{% if var('FacebookAdsByPlatformPosition') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('fbads_by_platform_position_tbl_ptrn'),
exclude=var('fbads_by_platform_position_tbl_exclude_ptrn'),
database=var('raw_database')) %}

{% for i in relations %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =replace(i,'`','').split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =replace(i,'`','').split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

{% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours') %}
            {% set hr = var('raw_table_timezone_offset_hours')[i] %}
        {% else %}
            {% set hr = 0 %}
        {% endif %}
        
    select 
    '{{brand|replace("`","")}}' as brand,
    '{{store|replace("`","")}}' as store,
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
        CAST(clicks as BIGINT) as clicks,
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
        CAST(coalesce(date_start,'NA')as Date) date_start,
        CAST(date_stop as DATE) date_stop,
        frequency,
        CAST(impressions as BIGINT) as impressions,
        CAST(inline_link_clicks as BIGINT) as inline_link_clicks,
        inline_post_engagement,
        objective,
        reach,
        spend,
        CAST(unique_clicks as BIGINT) as unique_clicks,
        unique_ctr,
        unique_inline_link_click_ctr,
        CAST(unique_inline_link_clicks as BIGINT) as unique_inline_link_clicks,

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
        
        coalesce(publisher_platform,'NA') publisher_platform,
        coalesce(platform_position,'NA') platform_position,
        
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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
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
    WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('fbads_by_platform_position_lookback') }},0) from {{ this }})
        {% endif %}    
        qualify DENSE_RANK() OVER (PARTITION BY ad_id,date_start,publisher_platform,platform_position order by a.{{daton_batch_runtime()}} desc) = 1 
{% if not loop.last %} union all {% endif %}
{% endfor %}   
{% if var('FacebookAds') %}
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
{{set_table_name('%facebookads%ads')}}    
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
        coalesce(a.id,'NA') as ad_id,
        a.account_id,
        adset_id,
        bid_amount,
        campaign_id,
        configured_status,
        CAST(a.created_time as timestamp) created_time,
        a.name,
        source_ad_id,
        a.status,

        {{extract_nested_value("creative","body","string")}} as creative_body,
        {{extract_nested_value("creative","image_url","string")}} as creative_image_url,
        {{extract_nested_value("creative","id","string")}} as creative_id,
        {{extract_nested_value("creative","account_id","string")}} as creative_account_id,
        {{extract_nested_value("creative","actor_id","string")}} as creative_actor_id,
        {{extract_nested_value("creative","applink_treatment","string")}} as creative_applink_treatment,
        {{extract_nested_value("creative","call_to_action_type","string")}} as creative_call_to_action_type,
        {{extract_nested_value("creative","effective_instagram_story_id","string")}} as creative_effective_instagram_story_id,
        {{extract_nested_value("creative","effective_object_story_id","string")}} as creative_effective_object_story_id,
        {{extract_nested_value("creative","title","string")}} as creative_title,
        {{extract_nested_value("creative","name","string")}} as creative_name,
        {{extract_nested_value("creative","instagram_permalink_url","string")}} as creative_instagram_permalink_url,
        {{extract_nested_value("creative","instagram_story_id","string")}} as creative_instagram_story_id,
        {{extract_nested_value("creative","link_og_id","string")}} as creative_link_og_id,
        {{extract_nested_value("creative","object_id","string")}} as creative_object_id,
        {{extract_nested_value("creative","object_story_id","string")}} as creative_object_story_id,
        {{extract_nested_value("creative","object_type","string")}} as creative_object_type,
        {{extract_nested_value("creative","object_url","string")}} as creative_object_url,
        {{extract_nested_value("creative","product_set_id","string")}} as creative_product_set_id,
        {{extract_nested_value("creative","status","string")}} as creative_status,
        {{extract_nested_value("creative","template_url","string")}} as creative_template_url,
        {{extract_nested_value("creative","thumbnail_url","string")}} as creative_thumbnail_url,
        {{extract_nested_value("creative","image_hash","string")}} as creative_image_hash,
        {{extract_nested_value("creative","url_tags","string")}} as creative_url_tags,
        {{extract_nested_value("creative","video_id","string")}} as creative_video_id,
        {{extract_nested_value("creative","link_url","string")}} as creative_link_url,

        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY a.id order by a.{{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
                {{unnesting("creative")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}

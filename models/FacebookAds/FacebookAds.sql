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
        a.id as ad_id,
        a.account_id,
        {% if target.type =='snowflake' %}
        adlabels.VALUE:id as adlabels_id,
        account.VALUE:id as adlabels_account_id,
        account.VALUE:account_id as adlabels_account_account_id,
        account.VALUE:account_status as adlabels_account_account_status,
        adlabels.VALUE:created_time as adlabels_created_time,
        adlabels.VALUE:name as adlabels_name,
        adlabels.VALUE:updated_time as adlabels_updated_time,
        {% else %}
        adlabels.id as adlabels_id,
        account.id as adlabels_account_id,
        account.account_id as adlabels_account_account_id,
        account.account_status as adlabels_account_account_status,
        adlabels.created_time as adlabels_created_time,
        adlabels.name as adlabels_name,
        adlabels.updated_time as adlabels_updated_time,
        {% endif %}
        adset_id,
        bid_amount,
        campaign_id,
        configured_status,
        CAST(a.created_time as timestamp) created_time,
        a.name,
        source_ad_id,
        a.status,
        {% if target.type =='snowflake' %}
        creative.VALUE:body as creative_body,
        creative.VALUE:image_url as creative_image_url,
        creative.VALUE:id as creative_id,
        creative.VALUE:account_id as creative_account_id,
        creative.VALUE:actor_id as creative_actor_id,
        creative.VALUE:applink_treatment as creative_applink_treatment,
        creative.VALUE:call_to_action_type as creative_call_to_action_type,
        creative.VALUE:effective_instagram_story_id as creative_effective_instagram_story_id,
        creative.VALUE:effective_object_story_id as creative_effective_object_story_id,
        creative.VALUE:title as creative_title,
        creative.VALUE:name as creative_name,
        creative.VALUE:instagram_permalink_url as creative_instagram_permalink_url,
        creative.VALUE:instagram_story_id as creative_instagram_story_id,
        creative.VALUE:link_og_id as creative_link_og_id,
        creative.VALUE:object_id as creative_object_id,
        creative.VALUE:object_story_id as creative_object_story_id,
        creative.VALUE:object_type as creative_object_type,
        creative.VALUE:object_url as creative_object_url,
        creative.VALUE:product_set_id as creative_product_set_id,
        creative.VALUE:status as creative_status,
        creative.VALUE:template_url as creative_template_url,
        creative.VALUE:thumbnail_url as creative_thumbnail_url,
        creative.VALUE:image_hash as creative_image_hash,
        creative.VALUE:url_tags as creative_url_tags,
        creative.VALUE:video_id as creative_video_id,
        creative.VALUE:link_url as creative_link_url,
        {% else %}
        creative.body as creative_body,
        creative.image_url as creative_image_url,
        creative.id as creative_id,
        creative.account_id as creative_account_id,
        creative.actor_id as creative_actor_id,
        creative.applink_treatment as creative_applink_treatment,
        creative.call_to_action_type as creative_call_to_action_type,
        creative.effective_instagram_story_id as creative_effective_instagram_story_id,
        creative.effective_object_story_id as creative_effective_object_story_id,
        creative.title as creative_title,
        creative.name as creative_name,
        creative.instagram_permalink_url as creative_instagram_permalink_url,
        creative.instagram_story_id as creative_instagram_story_id,
        creative.link_og_id as creative_link_og_id,
        creative.object_id as creative_object_id,
        creative.object_story_id as creative_object_story_id,
        creative.object_type as creative_object_type,
        creative.object_url as creative_object_url,
        creative.product_set_id as creative_product_set_id,
        creative.status as creative_status,
        creative.template_url as creative_template_url,
        creative.thumbnail_url as creative_thumbnail_url,
        creative.image_hash as creative_image_hash,
        creative.url_tags as creative_url_tags,
        creative.video_id as creative_video_id,
        creative.link_url as creative_link_url,
        {% endif %}
        a.{{daton_user_id()}} as _daton_user_id,
        a.{{daton_batch_runtime()}} as _daton_batch_runtime,
        a.{{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY a.id order by a.{{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
                {{unnesting("adlabels")}}
                {{multi_unnesting("adlabels","account")}}
                {{unnesting("creative")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}
        )
        where row_num = 1

    {% if not loop.last %} union all {% endif %}
{% endfor %}

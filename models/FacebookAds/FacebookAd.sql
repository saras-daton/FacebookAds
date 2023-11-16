{% if var('FacebookAd') %}
    {{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('fbads_tbl_ptrn'),
exclude=var('fbads_tbl_exclude_ptrn'),
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

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        FROM  {{i}} a
                {{unnesting("creative")}}
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                
    WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('fbads_lookback') }},0) from {{ this }})
        {% endif %}    
        qualify DENSE_RANK() OVER (PARTITION BY a.id order by a.{{daton_batch_runtime()}} desc) = 1  

{% if not loop.last %} union all {% endif %}
{% endfor %}   

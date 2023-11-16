{% if var('FacebookAdCreatives') %}
{{ config( enabled = True ) }}
{% else %}
    {{ config( enabled = False ) }}
{% endif %}


{% set relations = dbt_utils.get_relations_by_pattern(
schema_pattern=var('raw_schema'),
table_pattern=var('fbads_creatives_tbl_ptrn'),
exclude=var('fbads_creatives_tbl_exclude_ptrn'),
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
        body,
        image_url,
        id,
        account_id,
        actor_id,
        applink_treatment,
        call_to_action_type,
        coalesce(effective_instagram_story_id,'NA') as effective_instagram_story_id,
        effective_object_story_id,
        title,
        coalesce(name,'NA') as name,
        instagram_permalink_url,
        instagram_story_id,
        link_og_id,
        object_id,
        object_story_id,
        object_type,
        object_url,
        product_set_id,
        status,
        template_url,
        thumbnail_url,
        image_hash,
        url_tags,
        video_id,
        link_url,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        FROM  {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                
    WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('fbads_creatives_lookback') }},0) from {{ this }})
        {% endif %}    
        qualify DENSE_RANK() OVER (PARTITION BY effective_object_story_id,name order by {{daton_batch_runtime()}} desc) = 1  

{% if not loop.last %} union all {% endif %}
{% endfor %}   

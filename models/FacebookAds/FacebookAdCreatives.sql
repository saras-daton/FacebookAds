{% if var('FacebookAdCreatives') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("fbads_creatives_tbl_ptrn","fbads_creatives_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        body,
        image_url,
        id,
        account_id,
        actor_id,
        applink_treatment,
        call_to_action_type,
        effective_instagram_story_id,
        effective_object_story_id,
        title,
        name,
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
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        DENSE_RANK() OVER (PARTITION BY effective_object_story_id,name order by {{daton_batch_runtime()}} desc) row_num
        FROM  {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('fbads_creatives_lookback') }},0) from {{ this }})
            {% endif %}    
        qualify DENSE_RANK() OVER (PARTITION BY effective_object_story_id,name order by {{daton_batch_runtime()}} desc) = 1  

{% if not loop.last %} union all {% endif %}
{% endfor %}   
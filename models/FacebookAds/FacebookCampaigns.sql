{% if var('FacebookCampaigns') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}

{# /*--calling macro for tables list and remove exclude pattern */ #}
{% set result =set_table_name("fbads_campaigns_tbl_ptrn","fbads_campaigns_tbl_exclude_ptrn") %}
{# /*--iterating through all the tables */ #}
{% for i in result %}

        select 
        {{ extract_brand_and_store_name_from_table(i, var('brandname_position_in_tablename'), var('get_brandname_from_tablename_flag'), var('default_brandname')) }} as brand,
        {{ extract_brand_and_store_name_from_table(i, var('storename_position_in_tablename'), var('get_storename_from_tablename_flag'), var('default_storename')) }} as store,
        id as campaign_id,
        account_id,
        --adlabels,
        name,
        objective,
        effective_status,
        buying_type,
        spend_cap,       
        {% if target.type == 'snowflake'%}
            {{timezone_conversion("TO_TIMESTAMP_TZ(start_time, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')")}} as start_time,
            {{timezone_conversion("TO_TIMESTAMP_TZ(updated_time, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')")}} as updated_time,
        {% else %}  
            {{timezone_conversion("PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_time)")}} as start_time,
            {{timezone_conversion("PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', updated_time)")}} as updated_time,
        {% endif %}
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id
        from  {{i}} a
                {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE a.{{daton_batch_runtime()}}  >= (select coalesce(max(_daton_batch_runtime) - {{ var('fbads_campaigns_lookback') }},0) from {{ this }})
                {% endif %}    
        qualify DENSE_RANK() OVER (PARTITION BY a.id order by {{daton_batch_runtime()}} desc) = 1 
{% if not loop.last %} union all {% endif %}
{% endfor %}   
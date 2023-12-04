{% macro set_exclude_table_name(variable) %}

    {% if target.type =='snowflake' %}
    and lower(table_name) not like '{{var(variable)}}' and table_schema='{{ var("raw_schema") }}'
    {% else %}
    and lower(table_name) not like '{{var(variable)}}'
    {% endif %}

{% endmacro %}
{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS", false) %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;
        {% endset %}
        {% do run_query(sql) %}
        {{- fsc_utils.create_udfs() -}}
    {% endif %}
{% endmacro %}
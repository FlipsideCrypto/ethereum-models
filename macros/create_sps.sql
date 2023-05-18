{% macro create_sps() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% if target.database == 'ETHEREUM' %}
            CREATE SCHEMA IF NOT EXISTS _internal;
            CREATE SCHEMA IF NOT EXISTS _datashare;
            {{ sp_create_prod_clone('_internal') }};
        {% endif %}
    {% endif %}
{% endmacro %}
{% macro create_sps() %}
    {% if target.database == 'ETHEREUM' %}
        CREATE SCHEMA IF NOT EXISTS _internal;
        {{ sp_create_prod_clone('_internal') }};
    {% endif %}

    {{ sp_run_load_nft_metadata() }};
{% endmacro %}
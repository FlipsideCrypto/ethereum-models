{% macro create_udfs() %}
    {% set sql %}
    CREATE schema if NOT EXISTS silver;
{{ create_js_hex_to_int() }};
{{ create_udf_hex_to_int(
        schema = "public"
    ) }}

    {% endset %}
    {% do run_query(sql) %}
    {% if target.database != "ETHEREUM_COMMUNITY_DEV" %}
        {% set sql %}
        {{ create_udf_load_nft_metadata() }}
        {{ create_udf_get_token_balances() }}
        {{ create_udf_get_eth_balances() }}
        {{ create_udf_get_reads() }}

        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}

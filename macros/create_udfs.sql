{% macro create_udfs() %}
    {% set sql %}
    CREATE schema if NOT EXISTS silver;
{{ create_js_hex_to_int() }};
{{ create_udf_hex_to_int(
        schema = "public"
    ) }}
    {{ create_udf_hex_to_int_with_inputs(
        schema = "public"
    ) }}
    {{ create_udtf_get_base_table(
        schema = "streamline"
    ) }}

    {% endset %}
    {% do run_query(sql) %}
    {% if target.database != "ETHEREUM_COMMUNITY_DEV" %}
        {% set sql %}
        {{ create_udf_get_chainhead() }}
        {{ create_udf_get_beacon_chainhead() }}
        {{ create_udf_call_node() }}
        {{ create_udf_load_nft_metadata() }}
        {{ create_udf_get_token_balances() }}
        {{ create_udf_get_eth_balances() }}
        {{ create_udf_get_reads() }}
        {{ create_udf_get_contract_abis() }}
        {{ create_udf_get_blocks() }}
        {{ create_udf_get_transactions() }}
        {{ create_udf_get_beacon_blocks() }}

        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}

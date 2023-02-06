{% macro create_udfs() %}
    {% if var("UPDATE_UDFS_AND_SPS") %}
        {% set sql %}
        CREATE schema if NOT EXISTS silver;
{{ create_js_hex_to_int() }};
{{ create_udf_hex_to_int(
            schema = "public"
        ) }}
        {{ create_udf_transform_logs(
            schema = 'silver'
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
            {{ create_udf_call_eth_node() }}
            {{ create_udf_call_node() }}
            {{ create_udf_call_read_batching() }}
            {{ create_udf_api() }}
            {{ create_udf_load_nft_metadata() }}
            {{ create_udf_get_token_balances() }}
            {{ create_udf_get_eth_balances() }}
            {{ create_udf_get_reads() }}
            {{ create_udf_get_contract_abis() }}
            {{ create_udf_get_blocks() }}
            {{ create_udf_get_transactions() }}
            {{ create_udf_get_beacon_blocks() }}
            {{ create_udf_decode_array_string() }}
            {{ create_udf_decode_array_object() }}
            {{ create_udf_rest_api() }}
            {{ create_udf_bulk_decode_logs() }}
            {{ create_udf_json_rpc() }}

            {% endset %}
            {% do run_query(sql) %}
        {% endif %}
    {% endif %}
{% endmacro %}

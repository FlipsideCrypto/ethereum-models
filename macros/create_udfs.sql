{% macro create_udfs() %}
    {% set sql %}
    CREATE schema if NOT EXISTS silver;
{{ create_js_hex_to_int() }};
{{ create_udf_load_nft_metadata() }};
{{ create_udf_hex_to_int(
        schema = "public"
    ) }}
    {{ create_udf_get_token_balances() }}
    {{ create_udf_get_eth_balances() }}

    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

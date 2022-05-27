{% macro create_udfs() %}
    {% set sql %}
    CREATE schema if NOT EXISTS silver;
{{ create_js_hex_to_int() }};
{{ create_udf_load_nft_metadata() }};
{{ create_udf_hex_to_int(
        schema = "public"
    ) }}

    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}

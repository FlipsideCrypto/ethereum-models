{% macro create_udfs() %}
    CREATE schema if NOT EXISTS silver;
{{ js_hex_to_int() }};
{% set sql %}
    {{ udf_load_nft_metadata() }};
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}

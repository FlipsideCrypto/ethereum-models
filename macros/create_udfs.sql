{% macro create_udfs() %}
    CREATE schema if NOT EXISTS silver_ethereum_2022;
{{ js_hex_to_int() }};
{% endmacro %}

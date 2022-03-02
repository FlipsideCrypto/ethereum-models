{% macro create_udfs() %}
    CREATE schema if NOT EXISTS silver;
{{ js_hex_to_int() }};
{% endmacro %}

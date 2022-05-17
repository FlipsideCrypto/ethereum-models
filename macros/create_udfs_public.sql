{% macro create_udfs_public() %}
    CREATE schema if NOT EXISTS PUBLIC;
{{ js_hex_to_int() }};
{% endmacro %}

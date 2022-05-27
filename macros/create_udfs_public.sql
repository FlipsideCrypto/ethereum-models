{% macro create_udfs_public() %}
    CREATE schema if NOT EXISTS PUBLIC;
{{ create_js_hex_to_int() }}
    {{ create_udf_hex_to_int() }};
{% endmacro %}

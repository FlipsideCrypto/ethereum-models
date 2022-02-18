{% macro create_udfs() %}
    CREATE schema if NOT EXISTS {{ target.schema }};
{{ js_hex_to_int() }};
{% endmacro %}

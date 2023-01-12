{% macro run_bulk_decoder(target) %}
    {% if if_data_call_function(
            func = "{{this.schema}}.udf_bulk_decode_logs(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 20000000,'producer_limit_size', 20000000))",
            target = "{{this.schema}}.{{this.identifier}}"
        );
%}
        {% set sql %}
        {% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        {% endset %}
        {% do run_query(sql) %}
        [if_data_call_function( func = "{{this.schema}}.udf_bulk_decode_logs(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 20000000,'producer_limit_size', 20000000))", target = "{{this.schema}}.{{this.identifier}}" ),"call system$wait(" ~ var("WAIT", 400) ~ ")" ]
{% endmacro %}

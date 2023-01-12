{% macro if_data_call_wait(func, target) %}
        {{ if_data_call_function(func, target) }}
    {% if execute %}
        {% set query %}
            SELECT 1 FROM {{ target }} LIMIT 1
        {% endset %}
        {% set result = run_query(query) %}
        {% if result[0][0] == 1 %}
            {% set wait_query %}
                CALL SYSTEM$WAIT({{ var("WAIT", 600) }})
            {% endset %}
            {% do run_query(wait_query) %}
        {% endif %}
    {% endif %}
{% endmacro %}

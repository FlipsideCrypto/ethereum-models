{% macro if_data_call_function(
        func,
        target
    ) %}
    {% if var(
            "STREAMLINE_INVOKE_STREAMS"
        ) %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: Calling udf " ~ func ~ " on " ~ target,
                True
            ) }}
        {% endif %}
    SELECT
        {{ func }}
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                {{ target }}
            LIMIT
                1
        )
    {% else %}
        {% if execute %}
            {{ log(
                "Running macro `if_data_call_function`: NOOP",
                False
            ) }}
        {% endif %}
    SELECT
        NULL
    {% endif %}
{% endmacro %}

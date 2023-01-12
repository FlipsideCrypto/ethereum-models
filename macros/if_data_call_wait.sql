{% macro if_data_call_wait(
        func,
        target
    ) %}
    {{ if_data_call_function(
        func,
        target
    )}};

    SELECT
        system$wait({{ var("WAIT", 600) }})
    WHERE
        EXISTS(
            SELECT
                1
            FROM
                {{ target }}
            LIMIT
                1
        );

{% endmacro %}

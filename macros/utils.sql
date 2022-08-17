{% macro if_data_call_function(
        func,
        target
    ) %}
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
{% endmacro %}

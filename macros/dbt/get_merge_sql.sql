{% macro get_merge_sql(
        target,
        source,
        unique_key,
        dest_columns,
        incremental_predicates
    ) -%}
    {% set predicate_override = "" %}
    {% if incremental_predicates [0] == "dynamic_range" %}
        -- run some queries to dynamically determine the min + max of this 'input_column' in the new data
        {% set input_column = incremental_predicates [1] %}
        {% set get_limits_query %}
    SELECT
        MIN(
            {{ input_column }}
        ) AS lower_limit,
        MAX(
            {{ input_column }}
        ) AS upper_limit
    FROM
        {{ source }}

        {% endset %}
        {% set limits = run_query(get_limits_query) [0] %}
        {% set lower_limit,
        upper_limit = limits [0],
        limits [1] %}
        -- use those calculated min + max values to limit 'target' scan, to only the days with new data
        {% set predicate_override %}
        dbt_internal_dest.{{ input_column }} BETWEEN '{{ lower_limit }}'
        AND '{{ upper_limit }}' {% endset %}
    {% endif %}

    {% set predicates = [predicate_override] if predicate_override else incremental_predicates %}
    -- standard merge from here
    {% set merge_sql = dbt.get_merge_sql(
        target,
        source,
        unique_key,
        dest_columns,
        predicates
    ) %}
    {{ return(merge_sql) }}
{% endmacro %}

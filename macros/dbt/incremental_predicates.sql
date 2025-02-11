{% macro standard_predicate(
        input_column = 'block_number'
    ) -%}
    {%- set database_name = target.database -%}
    {%- set schema_name = generate_schema_name(
        node = model
    ) -%}
    {%- set table_name = generate_alias_name(
        node = model
    ) -%}
    {%- set tmp_table_name = table_name ~ '__dbt_tmp' -%}
    {%- set full_table_name = database_name ~ '.' ~ schema_name ~ '.' ~ table_name -%}
    {%- set full_tmp_table_name = database_name ~ '.' ~ schema_name ~ '.' ~ tmp_table_name -%}
    {{ full_table_name }}.{{ input_column }} >= (
        SELECT
            MIN(
                {{ input_column }}
            )
        FROM
            {{ full_tmp_table_name }}
    )
{%- endmacro %}

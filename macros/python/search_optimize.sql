{% macro search_opt(equality=True, substring=True, columns=[], equality_columns=[], substring_columns=[]) %}
{% if '_dev' not in target.database.lower() %}
    {% set columns_list = columns | join(', ') %}
    {% set equality_cols = equality_columns | join(', ') %}
    {% set substring_cols = substring_columns | join(', ') %}

    {% if equality_columns and not substring_columns %}
        {% do exceptions.raise_compiler_error("You specified equality_columns without substring_columns. Please specify both column sets.") %}
    {% elif substring_columns and not equality_columns %}
        {% do exceptions.raise_compiler_error("You specified substring_columns without equality_columns. Please specify both column sets.") %}
    {% elif equality_columns and substring_columns %}
        ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY({{ equality_cols }}), SUBSTRING({{ substring_cols }})
    {% elif equality and not substring %}
        ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY({{ columns_list }})
    {% elif not equality and substring %}
        ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON SUBSTRING({{ columns_list }})
    {% elif equality and substring and not equality_columns and not substring_columns %}
        ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY({{ columns_list }}), SUBSTRING({{ columns_list }})
    {% elif not equality and not substring and not columns and not equality_columns and not substring_columns%}
        ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION
    {% else %}
        {% do exceptions.raise_compiler_error("You specified an invalid combination of parameters. Please refer to the documentation.") %}
    {% endif %}
{% endif %}
{% endmacro %}
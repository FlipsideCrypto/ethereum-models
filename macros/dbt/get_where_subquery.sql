{% macro get_where_subquery(relation) -%}
    {# Call the fsc_evm version of get_where_subquery #}
    {{ return(fsc_evm.get_where_subquery(relation)) }}
{%- endmacro %}
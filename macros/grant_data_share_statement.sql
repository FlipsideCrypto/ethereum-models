{% macro grant_data_share_statement(table_name, resource_type) %}
  {% if target.database == 'ETHEREUM' %}
    GRANT SELECT ON {{ resource_type }} ETHEREUM.CORE.{{ table_name }} TO SHARE "FLIPSIDE_ETHEREUM";
  {% else %}
    select 1; -- hooks will error if they don't have valid SQL in them, this handles that!
  {% endif %}
{% endmacro %}
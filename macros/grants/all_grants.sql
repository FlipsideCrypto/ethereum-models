{% macro apply_grants(schemas) %}
        {% if target.name == "prod" %}
                {{ grant_ownership_on_schemas(
                        schemas,
                        'DBT_CLOUD_ETHEREUM'
                ) }}
                {# {{ grant_all_on_schemas(
                schemas,
                'DBT_CLOUD_ETHEREUM'
) }}
#}
{{ grant_select_on_schemas(
        schemas,
        'INTERNAL_DEV'
) }}
{% else %}
        {{ grant_ownership_on_schemas(
                schemas,
                'INTERNAL_DEV'
        ) }}
{% endif %}
{% endmacro %}

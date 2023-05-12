
{% set dag = {} %}
{% for key, value in graph.nodes.items() -%}
    {%
    if value.refs
    and set(value.fqn).intersection(["gold"])
    and value.config.materialized == "view"
    and value.config.enabled
    and not value.sources
    -%}
    ==================================================================
    {{ key }}
    {{ value.database }}.{{ value.schema }}.{{ value.alias }}
    {%- set name = value.database + "." + value.schema + "." + value.alias %}
    {{value.config.materialized }}
    {{ value["fqn"]}}
    ------------------------------------------------------------------
    Dependencies:
    {{ value.depends_on.nodes | unique | list | pprint }}
    ALL ANCESTORS:
        {% set ancestors =  fromjson("[" ~ get_ancestors(value, include_depth=true, exclude_source=false)[:-1] ~ "]") -%}
    {{- ancestors | pprint -}}
    {# build dictionary[db_object, dag] #}
    {%- set _result = fromjson("[" ~ get_ancestors(value, exclude_source=true)[:-1] ~ "]") %}
        {% if _result %}
            {% do _result.insert(0, key) %}
            {% do dag.update({name : _result | reverse|list})  %}
        {% else %}
            {% do dag.update({name : [key] })  %}
        {%- endif %}
    Refs:
    {%- set not_ephemeral = [] -%}
    {% for item in value.refs if not item[0].startswith("_") %}
        {%- set _ = not_ephemeral.append(item) -%}
    {%- endfor -%}
    {{ not_ephemeral | pprint}}
    Sources:
    {{ value.sources | pprint }}

    {%- endif %}
{%- endfor %}

total views: {{ views | length}}
view with most dependencies: {{ views | sort(reverse=True) | first  | pprint }}

{{ dag.items() | length }}
'ETHEREUM_DEV.core.ez_nft_sales'
{{ dag['ETHEREUM_DEV.core.ez_nft_sales'] | pprint }}


 {{fromyaml("[" ~ get_ancestors(graph.nodes["model.ethereum_models.uniswapv3__ez_lp_actions"], exclude_source=true) ~ "]" )}}
 {{fromyaml("[" ~ get_ancestors(graph.nodes["model.ethereum_models.uniswapv3__ez_lp_actions"], exclude_source=false) ~ "]" )}}
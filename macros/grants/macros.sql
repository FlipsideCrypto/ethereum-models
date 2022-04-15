{% macro grant_select_on_schemas(
                schemas,
                role
        ) %}
        {% for schema in schemas %}
                GRANT usage
                ON schema {{ schema }} TO role {{ role }};
GRANT
        SELECT
                ON ALL tables IN schema {{ schema }} TO role {{ role }};
GRANT
        SELECT
                ON ALL VIEWS IN schema {{ schema }} TO role {{ role }};
GRANT
        SELECT
                ON future tables IN schema {{ schema }} TO role {{ role }};
GRANT
        SELECT
                ON future VIEWS IN schema {{ schema }} TO role {{ role }};
        {% endfor %}
{% endmacro %}

{% macro grant_all_on_schemas(
                schemas,
                role
        ) %}
        {% for schema in schemas %}
                GRANT usage
                ON schema {{ schema }} TO role {{ role }};
GRANT ALL
                ON ALL tables IN schema {{ schema }} TO role {{ role }};
GRANT ALL
                ON ALL VIEWS IN schema {{ schema }} TO role {{ role }};
GRANT ALL
                ON future tables IN schema {{ schema }} TO role {{ role }};
GRANT ALL
                ON future VIEWS IN schema {{ schema }} TO role {{ role }};
        {% endfor %}
{% endmacro %}

{% macro grant_ownership_on_schemas(
                schemas,
                role
        ) %}
        {% for schema in schemas %}
                GRANT ownership
                ON schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON ALL tables IN schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON future tables IN schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON ALL VIEWS IN schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON future VIEWS IN schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON ALL procedures IN schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON future procedures IN schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON ALL functions IN schema {{ schema }} TO role {{ role }};
GRANT ownership
                ON future functions IN schema {{ schema }} TO role {{ role }};
        {% endfor %}
{% endmacro %}

{% macro create_aws_ethereum_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_ethereum_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::661245089684:role/snowflake-api-ethereum' api_allowed_prefixes = (
            'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/',
            'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}

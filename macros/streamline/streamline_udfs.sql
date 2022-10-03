{% macro create_udf_get_token_balances() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_streamline_produce() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/streamline_produce'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/streamline_produce'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_eth_balances() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_streamline_produce_dlqs() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/streamline_produce_dlqs'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/streamline_produce_dlqs'
    {%- endif %};
{% endmacro %}

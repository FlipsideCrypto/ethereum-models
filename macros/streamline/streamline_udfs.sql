{% macro create_udf_get_token_balances_realtime() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_token_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_token_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_token_balances'
    {%- endif %}

    headers = (
        'source' = '{{ ref("streamline__token_balances_realtime") }}'
    );
{% endmacro %}

{% macro create_udf_get_token_balances_history() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_token_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_token_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_token_balances'
    {%- endif %}

    headers = (
        'source' = '{{ ref("streamline__token_balances_history") }}'
    );
{% endmacro %}

{% macro create_udf_get_eth_balances_realtime() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_eth_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_eth_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_eth_balances'
    {%- endif %}

    headers = (
        'source' = '{{ ref("streamline__eth_balances_realtime") }}'
    );
{% endmacro %}

{% macro create_udf_get_eth_balances_history() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_eth_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_eth_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_eth_balances'
    {%- endif %}

    headers = (
        'source' = '{{ ref("streamline__eth_balances_history") }}'
    );
{% endmacro %}

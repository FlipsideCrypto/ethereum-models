{% macro create_udf_get_token_balances() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_token_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_token_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_token_balances'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_eth_balances() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_eth_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_eth_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_eth_balances'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_reads() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_reads() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_reads'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_reads'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_contract_abis() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_contract_abis() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_contract_abis'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_contract_abis'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_committees() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_beacon_blocks() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_committees'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_committees'
    {%- endif %};
{% endmacro %}
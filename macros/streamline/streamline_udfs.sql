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

{% macro create_udf_get_blocks() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_blocks() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_blocks'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_blocks'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_transactions() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_transactions() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_transactions'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_transactions'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_beacon_blocks() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_beacon_blocks() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_beacon_blocks'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_beacon_blocks'
    {%- endif %};
{% endmacro %}

-- {% macro create_udf_get_chainhead() %}
--     CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
--         'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
--     {% else %}
--         'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
--     {%- endif %};
-- {% endmacro %}

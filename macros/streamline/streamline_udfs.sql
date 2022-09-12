{% macro create_udf_get_token_balances() %}
{% if target.name == "prod" %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_token_balances() returns text api_integration = aws_ethereum_api AS
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_token_balances'
    {% else %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_token_balances() returns text api_integration = aws_ethereum_api AS
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_token_balances'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_eth_balances() %}
{% if target.name == "prod" %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_eth_balances() returns text api_integration = aws_ethereum_api AS
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_eth_balances'
    {% else %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_eth_balances() returns text api_integration = aws_ethereum_api AS
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_eth_balances'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_reads() %}
    {% if target.name == "prod" %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_reads() returns text api_integration = aws_ethereum_api AS
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_reads'
    {% else %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_reads() returns text api_integration = aws_ethereum_api AS
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_reads'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_contract_abis() %}
    {% if target.name == "prod" %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_contract_abis() returns text api_integration = aws_ethereum_api AS
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_contract_abis'
    {% else %}
    CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_contract_abis() returns text api_integration = aws_ethereum_api AS
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_contract_abis'
    {%- endif %};
{% endmacro %}

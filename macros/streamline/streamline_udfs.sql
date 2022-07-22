{% macro create_udf_get_token_balances() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_token_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_token_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_token_balances'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_eth_balances() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_eth_balances() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_eth_balances'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_eth_balances'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_rarible_nft_collections() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_rarible_nft_collections() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://6gh4ncj0ig.execute-api.us-east-1.amazonaws.com/prod/bulk_load_nft_collections/rarible/ethereum'
    {% else %}
        'https://rtcsra1z35.execute-api.us-east-1.amazonaws.com/dev/bulk_load_nft_collections/rarible/ethereum'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_rarible_nft_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_rarible_nft_metadata() returns text api_integration = aws_ethereum_api AS {% if target.name == "prod" %}
        'https://6gh4ncj0ig.execute-api.us-east-1.amazonaws.com/prod/bulk_load_nft_metadata/rarible/ethereum'
    {% else %}
        'https://rtcsra1z35.execute-api.us-east-1.amazonaws.com/dev/bulk_load_nft_metadata/rarible/ethereum'
    {%- endif %};
{% endmacro %}
{% macro create_udf_load_nft_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_load_nft_metadata() returns text api_integration = aws_nft_metadata_api_dev AS {% if target.name == "prod" -%}
        'https://6gh4ncj0ig.execute-api.us-east-1.amazonaws.com/prod/bulk_load_nft_metadata/ethereum'
    {% else %}
        'https://rtcsra1z35.execute-api.us-east-1.amazonaws.com/dev/bulk_load_nft_metadata/ethereum'
    {%- endif %}
{% endmacro %}

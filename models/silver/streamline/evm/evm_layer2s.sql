{{ config(
    materialized = 'view',
    tags = ['streamline_view'],
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_token_balances(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT 
    $1 as L2_NAME, 
    $2 as HOST,
    $3 as SECRET_SSM_KEY
  FROM VALUES 
    ('arbitrum', 'www.figment.com', 'ARBITRUM_SECRET'), 
    ('avalanche', 'www.figment.com', 'AVALANCHE_SECRET'), 
    ('bsc', 'www.figment.com', 'BSC_SECRET'), 
    ('ethereum', 'www.figment.com', 'ETHEREUM_SECRET'), 
    ('gnosis', 'www.figment.com', 'GNOSIS_SECRET'), 
    ('harmony', 'www.figment.com', 'HARMONY_SECRET'), 
    ('optimism', 'www.figment.com', 'OPTIMISM_SECRET'), 
    ('polygon', 'www.figment.com', 'POLYGON_SECRET');
{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_contracts']
) }}

SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata
FROM
    {{ ref('silver_ethereum_2022__contracts') }}

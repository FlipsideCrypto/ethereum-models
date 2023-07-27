{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['non_realtime']
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ ref('silver__prices') }}

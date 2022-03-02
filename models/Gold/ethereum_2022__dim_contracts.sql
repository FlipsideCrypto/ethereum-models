{{ config(
    materialized = 'view'
) }}

SELECT
    address,
    symbol,
    NAME,
    decimals,
    contract_metadata
FROM
    {{ ref('silver__contracts') }}

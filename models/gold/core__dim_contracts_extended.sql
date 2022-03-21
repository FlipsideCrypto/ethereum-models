{{ config(
    materialized = 'view'
) }}

SELECT
    system_created_at,
    block_number,
    block_timestamp,
    creator_address,
    contract_address,
    logic_address,
    token_convention,
    NAME,
    symbol,
    decimals,
    meta
FROM
    {{ ref('silver__contracts_extended') }}

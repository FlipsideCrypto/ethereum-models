{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
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
    contract_metadata
FROM
    {{ ref('silver__contracts_extended') }}

{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_type,
    contract_address AS nft_address,
    project_name,
    from_address AS nft_from_address,
    to_address AS nft_to_address,
    tokenId,
    erc1155_value
FROM
    {{ ref('silver__nft_transfers') }}

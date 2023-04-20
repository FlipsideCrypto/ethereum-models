{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

WITH nft_base AS (

    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        event_index,
        event_type,
        contract_address AS nft_address,
        from_address AS nft_from_address,
        to_address AS nft_to_address,
        tokenId,
        erc1155_value
    FROM
        {{ ref('silver__nft_transfers') }}
)
SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    event_type,
    nft_address,
    project_name,
    nft_from_address,
    nft_to_address,
    tokenId,
    token_metadata,
    erc1155_value
FROM
    nft_base b
    LEFT JOIN {{ ref('silver__nft_labels_temp') }}
    l
    ON b.nft_address = l.project_address
    AND b.tokenId = l.token_id

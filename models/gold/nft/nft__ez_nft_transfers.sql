{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    intra_event_index,
    event_type,
    contract_address AS nft_address,
    contract_address, 
    project_name,
    project_name AS name,
    from_address AS nft_from_address,
    from_address,
    to_address AS nft_to_address,
    to_address,
    tokenId,
    tokenId AS token_id,
    erc1155_value,
    COALESCE(erc1155_value, '1') ::STRING AS quantity,
    IFF(erc1155_value IS NULL, 'erc721', 'erc1155') AS token_standard,
    COALESCE (
        nft_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index','intra_event_index']
        ) }}
    ) AS ez_nft_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_transfers') }} 


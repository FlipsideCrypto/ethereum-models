{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }
    } }
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    intra_event_index,
    token_transfer_type,
    IFF(
        from_address = '0x0000000000000000000000000000000000000000',
        TRUE,
        FALSE
    ) AS is_mint, --new column
    from_address, --new column
    to_address, --new column
    contract_address, --new column
    tokenId AS token_id, --new column
    COALESCE(
        erc1155_value,
        '1'
    ) :: STRING AS quantity, --new column
    CASE
        WHEN token_transfer_type = 'erc721_Transfer' THEN 'erc721'
        WHEN token_transfer_type = 'erc1155_TransferSingle' THEN 'erc1155'
        WHEN token_transfer_type = 'erc1155_TransferBatch' THEN 'erc1155'
    END AS token_standard, --new column
    project_name AS NAME, --new column
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
    ) AS modified_timestamp,
    event_type, -- deprecate
    from_address AS nft_from_address, -- deprecate
    to_address AS nft_to_address, -- deprecate
    contract_address AS nft_address, --deprecate
    tokenId, -- deprecate
    erc1155_value, --deprecate
    project_name --deprecate
FROM
    {{ ref('silver__nft_transfers') }}

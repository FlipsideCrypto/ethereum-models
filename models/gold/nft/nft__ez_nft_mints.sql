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
    event_type,
    nft_address,
    project_name,
    nft_from_address,
    nft_to_address,
    tokenId,
    erc1155_value,
    mint_price_eth,
    mint_price_usd,
    nft_count,
    amount,
    amount_usd,
    mint_price_tokens,
    mint_price_tokens_usd,
    mint_token_symbol,
    mint_token_address,
    tx_fee,
    _log_id,
    _inserted_timestamp,
    COALESCE (
        nft_mints_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_log_id','nft_address']
        ) }}
    ) AS ez_nft_mints_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_mints') }}

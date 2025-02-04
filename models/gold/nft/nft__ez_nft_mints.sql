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
    nft_address AS contract_address,--new column
    project_name AS NAME,--new column
    nft_from_address AS from_address,--new column
    nft_to_address AS to_address,--new column
    tokenId AS token_id,--new column
    COALESCE(
        erc1155_value,
        '1'
    ) :: STRING AS quantity,--new column
    CASE
        WHEN erc1155_value IS NULL THEN 'erc721'
        ELSE 'erc1155'
    END AS token_standard,--new column
    mint_price_eth AS mint_price_native,--new column
    mint_price_usd,
    nft_count,
    amount,
    amount_usd,
    mint_price_tokens,
    mint_price_tokens_usd,
    mint_token_symbol,
    mint_token_address,
    tx_fee,
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
    ) AS modified_timestamp,
    nft_address,--deprecate
    project_name,--deprecate
    nft_from_address,--deprecate
    nft_to_address,--deprecate
    tokenId,--deprecate
    erc1155_value,--deprecate
    mint_price_eth,--deprecate
    _log_id,--deprecate
    _inserted_timestamp --deprecate
FROM
    {{ ref('silver__nft_mints') }}

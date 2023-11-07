{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
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
    token_metadata,
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
    tx_fee
FROM
    {{ ref('nft__ez_nft_mints') }}
WHERE
    block_timestamp :: DATE BETWEEN '2021-12-01'
    AND '2021-12-31'

{{ config(
    materialized = 'view'
) }}

SELECT
    block_number,
    block_timestamp,
    network,
    blockchain,
    tx_count,
    difficulty,
    total_difficulty,
    extra_data,
    gas_limit,
    gas_used,
    HASH,
    parent_hash,
    miner,
    nonce,
    receipts_root,
    sha3_uncles,
    SIZE,
    uncle_blocks
FROM
    {{ ref('silver__blocks') }}

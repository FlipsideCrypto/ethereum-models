{{ config(
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'gold_ethereum', 'ethereum_blocks']
) }}

SELECT
    block_id,
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
    uncles
FROM
    {{ ref('silver_ethereum_2022__blocks') }}

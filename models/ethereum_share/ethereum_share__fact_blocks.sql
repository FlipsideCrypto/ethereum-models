{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
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
    uncle_blocks,
    block_header_json
FROM
    {{ ref('silver__blocks') }}
    where block_timestamp::date between '2021-12-01' and '2021-12-31'
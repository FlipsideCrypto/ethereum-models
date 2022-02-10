{{ config (
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'bronze_ethereum', 'ethereum_transactions']
) }}

SELECT
    record_id,
    tx_id,
    tx_block_index,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx,
    ingested_at
FROM
    {{ source(
        'prod',
        'ethereum_txs'
    ) }}

{{ config (
    materialized = 'view'
) }}

WITH base_table AS (

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
),
block_hashes AS (
    SELECT
        block_number,
        HASH
    FROM
        {{ ref('silver__blocks') }}
),
remove_uncles AS (
    SELECT
        *
    FROM
        base_table
        INNER JOIN block_hashes
        ON block_hashes.block_number = COALESCE(
            tx :block_number :: INTEGER,
            tx :blockNumber :: INTEGER
        )
        AND block_hashes.hash = COALESCE(
            tx :block_hash :: STRING,
            tx :blockHash :: STRING
        )
)
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
    remove_uncles qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    ingested_at DESC)) = 1

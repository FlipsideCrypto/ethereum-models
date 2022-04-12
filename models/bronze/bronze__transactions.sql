{{ config (
    materialized = 'view'
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
WHERE
    ingested_at >= '2022-03-01' qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_block_index
ORDER BY
    ingested_at DESC)) = 1

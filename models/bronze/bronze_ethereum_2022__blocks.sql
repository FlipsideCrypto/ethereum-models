{{ config (
    materialized = 'view'
) }}

SELECT
    record_id,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx_count,
    header,
    ingested_at
FROM
    {{ source(
        'chainwalkers_prod',
        'ethereum_blocks'
    ) }}

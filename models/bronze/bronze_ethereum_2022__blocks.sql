{{ config (
    materialized = 'view',
    tags = ['snowflake', 'ethereum', 'bronze_ethereum', 'ethereum_blocks']
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
        'prod',
        'ethereum_blocks'
    ) }}

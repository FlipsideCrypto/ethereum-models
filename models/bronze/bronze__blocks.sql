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
        'prod',
        'ethereum_blocks'
    ) }}
WHERE
    CASE
        WHEN block_id <= 14348123
        AND block_id >= 14298399
        AND ingested_at < '2022-03-20' THEN FALSE
        ELSE TRUE
    END = TRUE

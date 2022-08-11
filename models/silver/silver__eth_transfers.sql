{{ config(
    materialized = 'view',
    tags = ['core']
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value,
    identifier,
    _call_id,
    ingested_at,
    input
FROM
    {{ ref('silver__traces') }}
WHERE
    TYPE = 'CALL'
    AND eth_value > 0
    AND tx_status = 'SUCCESS'
    AND output IS NOT NULL

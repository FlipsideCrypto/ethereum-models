{{ config (
    materialized = "ephemeral",
    unique_key = "block_number",
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    status
FROM
    {{ ref("silver__transactions") }}

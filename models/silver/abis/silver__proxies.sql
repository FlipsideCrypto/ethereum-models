{{ config (
    materialized = "table"
) }}

WITH base AS (

    SELECT
        from_address,
        to_address,
        MIN(block_number) AS start_block,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        TYPE = 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'
        AND from_address != to_address -- exclude self-calls
    GROUP BY
        from_address,
        to_address
)
SELECT
    from_address AS contract_address,
    to_address AS proxy_address,
    start_block,
    CONCAT(
        from_address,
        '-',
        to_address
    ) AS _id,
    _inserted_timestamp
FROM
    base

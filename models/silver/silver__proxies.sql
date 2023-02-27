{{ config (
    materialized = "table"
) }}

WITH base AS (

    SELECT
        from_address,
        to_address,
        MIN(block_number) AS start_block
    FROM
        {{ ref('silver__traces') }}
    WHERE
        TYPE = 'DELEGATECALL'
        AND tx_status = 'success'
        AND from_address != to_address -- exclude self-calls
        AND output :: STRING = '0x'
    GROUP BY
        from_address,
        to_address
)
SELECT
    from_address AS contract_address,
    to_address AS proxy_address,
    start_block,
    COALESCE(
        (LAG(start_block) over(PARTITION BY from_address
        ORDER BY
            start_block DESC)) - 1,
            10000000000
    ) AS end_block,
    CONCAT(
        from_address,
        '-',
        to_address
    ) AS _id
FROM
    base

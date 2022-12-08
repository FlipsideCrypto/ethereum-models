{{ config (
    materialized = "view"
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
)
SELECT
    block_number,
    _log_id,
    abi,
    DATA
FROM
    {{ ref("streamline__decode_logs") }}
WHERE
    (
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        OR block_number > 15000000
    )
    AND block_number IS NOT NULL
EXCEPT
SELECT
    block_number,
    _log_id,
    abi,
    DATA
FROM
    {{ ref("streamline__complete_decode_logs") }}
WHERE
    block_number >= (
        SELECT
            block_number
        FROM
            last_3_days
    )
    OR block_number > 15000000
UNION ALL
SELECT
    block_number,
    _log_id,
    abi,
    DATA
FROM
    {{ ref("streamline__decode_logs_history") }}

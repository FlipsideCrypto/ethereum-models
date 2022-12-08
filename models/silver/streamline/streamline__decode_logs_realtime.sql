{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_decode_logs(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )

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
    l.block_number,
    l._log_id,
    A.data AS abi,
    l.data
FROM
    {{ ref("streamline__decode_logs") }}
    l
    INNER JOIN {{ ref("silver__abis") }} A
    ON l.abi_address = A.contract_address
WHERE
    (
       l. block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        OR l.block_number > 15000000
    )
    AND l.block_number IS NOT NULL
    AND _log_id NOT IN (
        SELECT
            _log_id
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
            _log_id
        FROM
            {{ ref("streamline__decode_logs_history") }}
    )

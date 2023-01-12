{{ config (
    materialized = "view",
    enabled = false,
    post_hook = [if_data_call_function( func = "{{this.schema}}.udf_bulk_decode_logs(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 20000000,'producer_limit_size', 20000000))", target = "{{this.schema}}.{{this.identifier}}" ),"call system$wait(" ~ var("WAIT", 400) ~ ")" ]
) }}

WITH look_back AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 1
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
    LEFT JOIN {{ ref("streamline__complete_decode_logs") }} C USING (
        block_number,
        _log_id
    )
WHERE
    l.block_number < (
        SELECT
            block_number
        FROM
            look_back
    )
    AND l.block_number IS NOT NULL
    AND l.block_number >= 16000000
    AND c.block_number >= 16000000
    AND C._log_id IS NULL
LIMIT
    1000000

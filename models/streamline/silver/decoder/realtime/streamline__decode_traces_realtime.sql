{{ config (
    materialized = "view",
    tags = ['streamline_decoded_traces_realtime']
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
    t.block_number,
    t.tx_hash,
    t.trace_index,
    _call_id,
    A.abi AS abi,
    A.function_name AS function_name,
    CASE
        WHEN TYPE = 'DELEGATECALL' THEN from_address
        ELSE to_address
    END AS abi_address,
    t.input AS input,
    COALESCE(
        t.output,
        '0x'
    ) AS output
FROM
    {{ ref("silver__traces") }}
    t
    INNER JOIN {{ ref("silver__complete_function_abis") }} A
    ON A.parent_contract_address = abi_address
    AND LEFT(
        t.input,
        10
    ) = LEFT(
        A.function_signature,
        10
    )
    AND t.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    t.block_number >= (
        SELECT
            block_number
        FROM
            look_back
    )
    AND t.block_number IS NOT NULL
    AND t.block_timestamp >= DATEADD('day', -2, CURRENT_DATE())

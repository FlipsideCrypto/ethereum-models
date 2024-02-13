{{ config (
    materialized = "view",
    post_hook = [if_data_call_function( func = "{{this.schema}}.udf_bulk_decode_traces(object_construct('sql_source', '{{this.identifier}}','producer_batch_size', 500000,'producer_limit_size', 20000000))", target = "{{this.schema}}.{{this.identifier}}" ),"call system$wait(" ~ var("WAIT", 400) ~ ")" ],
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
    AND _call_id NOT IN (
        SELECT
            _call_id
        FROM
            {{ ref("streamline__complete_decode_traces") }}
        WHERE
            block_number >= (
                SELECT
                    block_number
                FROM
                    look_back
            )
            AND _inserted_timestamp >= DATEADD('day', -2, CURRENT_DATE()))
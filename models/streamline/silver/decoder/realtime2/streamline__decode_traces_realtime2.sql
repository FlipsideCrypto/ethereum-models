{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_traces_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"DECODED_TRACES", 
        "sql_limit" :"20000000",
        "producer_batch_size" :"500000",
        "worker_batch_size" :"500000",
        "sql_source" :"{{this.identifier}}" }
    ),
    "call system$wait(" ~ var("WAIT", 400) ~ ")" ],
    tags = ['streamline_decoded_traces_realtime']
) }}

WITH look_back AS (

    SELECT
        block_number
    FROM
        {{ ref("_24_hour_lookback") }}
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

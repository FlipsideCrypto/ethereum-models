{{ config (
    materialized = "view",
    post_hook = [fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_decode_logs_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"DECODED_LOGS",
        "sql_limit" :"20000000",
        "producer_batch_size" :"20000000",
        "worker_batch_size" :"200000",
        "sql_source" :"{{this.identifier}}" }
    ),
    fsc_utils.if_data_call_wait()],
    tags = ['streamline_decoded_logs_realtime']
) }}

WITH look_back AS (

    SELECT
        block_number
    FROM
        {{ ref("_24_hour_lookback") }}
)
SELECT
    l.block_number,
    l._log_id,
    A.abi AS abi,
    OBJECT_CONSTRUCT(
        'topics',
        l.topics,
        'data',
        l.data,
        'address',
        l.contract_address
    ) AS DATA
FROM
    {{ ref("silver__logs") }}
    l
    INNER JOIN {{ ref("silver__complete_event_abis") }} A
    ON A.parent_contract_address = l.contract_address
    AND A.event_signature = l.topics [0] :: STRING
    AND l.block_number BETWEEN A.start_block
    AND A.end_block
WHERE
    (
        l.block_number >= (
            SELECT
                block_number
            FROM
                look_back
        )
    )
    AND l.block_number IS NOT NULL
    AND l.block_timestamp >= DATEADD('day', -2, CURRENT_DATE())
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
                    look_back
            )
            AND _inserted_timestamp >= DATEADD('day', -2, CURRENT_DATE()))
LIMIT 100 --remove for prod
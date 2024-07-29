{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"receipts_v2",
        "sql_limit" :"100000",
        "producer_batch_size" :"100",
        "worker_batch_size" :"10",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result"]) }
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
to_do AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        (
            block_number >= (
                SELECT
                    block_number
                FROM
                    last_3_days
            )
        )
        AND block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_receipts") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
),
ready_blocks AS (
    SELECT
        block_number
    FROM
        to_do
    UNION
    SELECT
        block_number
    FROM
        (
            SELECT
                block_number
            FROM
                {{ ref("_missing_receipts") }}
            UNION
            SELECT
                block_number
            FROM
                {{ ref("_missing_txs") }}
            UNION
            SELECT
                block_number
            FROM
                {{ ref("_unconfirmed_blocks") }}
        )
)
SELECT
    block_number,
    ROUND(
        block_number,
        -3
    ) AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'eth_getBlockReceipts',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number))),
            'vault/prod/ethereum/quicknode/mainnet'
        ) AS request
        FROM
            ready_blocks
        ORDER BY
            block_number ASC
        LIMIT 10
            {# 300 #}

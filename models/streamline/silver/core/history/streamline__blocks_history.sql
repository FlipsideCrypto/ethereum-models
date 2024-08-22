{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks_v2",
        "sql_limit" :"50000",
        "producer_batch_size" :"1000",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["data", "result.transactions"]) }
    ),
    tags = ['streamline_core_history']
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
            block_number <= (
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
        {{ ref("streamline__complete_blocks") }}
        b
        INNER JOIN {{ ref("streamline__complete_transactions") }}
        t USING(block_number) -- inner join to ensure that only blocks with both block data and transaction data are excluded
    WHERE
        block_number <= (
            SELECT
                block_number
            FROM
                last_3_days
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
            'eth_getBlockByNumber',
            'params',
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), TRUE)),
            --set to TRUE for full txn data
            'vault/prod/ethereum/quicknode/mainnet'
        ) AS request
        FROM
            to_do
        ORDER BY
            partition_key ASC

{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"confirm_blocks_v2",
        "sql_limit" :"5000000",
        "producer_batch_size" :"5000",
        "worker_batch_size" :"500",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_core_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
),
look_back AS (
    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 6
),
tbl AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
        AND block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_confirmed_blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
        AND _inserted_timestamp >= DATEADD(
            'day',
            -4,
            SYSDATE()
        )
        AND block_number >= (
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
            ARRAY_CONSTRUCT(utils.udf_int_to_hex(block_number), FALSE)),
            'vault/prod/ethereum/quicknode/mainnet'
        ) AS request
        FROM
            tbl
        ORDER BY
            block_number ASC
LIMIT 600
